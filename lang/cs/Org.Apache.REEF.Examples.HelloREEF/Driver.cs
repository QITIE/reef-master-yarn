// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class Driver : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>, IObserver<IFailedEvaluator>, IObserver<IFailedTask>, IObserver<ICompletedTask>
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(Driver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private int _failureCount;
        private int _maxTrial = 1000;
        private int _numOfContainers;
        private int _numOfvCoresPerNode;
        private int _memoryPerNode;
        private string _testNodes;

        [Inject]
        private Driver(IEvaluatorRequestor evaluatorRequestor, CommandLineArguments args)
        {
            File.Copy(Common.GlobalPath + Common.GlobalConfigFileName,
                Common.GlobalConfigFileName);
            _evaluatorRequestor = evaluatorRequestor;
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var taskConfiguration  = TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, Utils.ReadStringArgumentFromConf(Common.GlobalConfigTaskName))
                        .Set(TaskConfiguration.Task, GenericType<JavaTask>.Class)
                        .Build();

            allocatedEvaluator.SubmitTask(taskConfiguration);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            _Logger.Log(Level.Info, string.Format("HelloDriver started at {0}", driverStarted.StartTime));
            _numOfContainers = Utils.ReadIntArgumentFromConf(Common.GlobalConfigNumOfClients);
            _memoryPerNode = Utils.ReadIntArgumentFromConf(Common.GlobalConfigClientMemory);
            _numOfvCoresPerNode = Utils.ReadIntArgumentFromConf(Common.GlobalConfigClientVCores);

            _testNodes = Utils.ReadStringArgumentFromConf(Common.GlobalConfigTestNodes);

            if (_testNodes.Equals("*"))
            {
                _testNodes = string.Empty;
            }

            _evaluatorRequestor.Submit(
                _evaluatorRequestor
                .NewBuilder()
                .SetNumber(_numOfContainers)
                .SetCores(_numOfvCoresPerNode)
                .SetMegabytes(_memoryPerNode)
                .SetNodeName(_testNodes)
                .Build());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="failedEvaluator"></param>
        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            Console.WriteLine("Receive a failed evaluator: " + failedEvaluator.Id);
            if (++_failureCount < _maxTrial)
            {
                Console.WriteLine("Requesting another evaluator");
                var newRequest =
                    _evaluatorRequestor.NewBuilder().SetNumber(1).SetCores(_numOfvCoresPerNode).SetMegabytes(_memoryPerNode).Build();
                _evaluatorRequestor.Submit(newRequest);
            }
            else
            {
                Console.WriteLine("Exceed max retries number");
                throw new Exception("Unrecoverable evaluator failure.");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="failedTask"></param>
        public void OnNext(IFailedTask failedTask)
        {
            string reason = string.Empty;

            if (failedTask.Reason != null)
            {
                reason = failedTask.Reason.IsPresent() ? failedTask.Reason.Value : string.Empty;
            }

            string description = string.Empty;

            if (failedTask.Description != null)
            {
                description = failedTask.Description.IsPresent() ? failedTask.Description.Value : string.Empty;
            }

            string data = string.Empty;

            if (failedTask.Data != null)
            {
                data = failedTask.Data.IsPresent() ? ByteUtilities.ByteArraysToString(failedTask.Data.Value) : string.Empty;
            }

            string errorMessage = string.Format(
                System.Globalization.CultureInfo.InvariantCulture,
                "Task [{0}] has failed caused by [{1}], with message [{2}] and description [{3}]. The raw data for failure is [{4}].",
                failedTask.Id,
                reason,
                failedTask.Message,
                description,
                data);

            Console.WriteLine(errorMessage);

            try
            {
                if (failedTask.GetActiveContext().IsPresent())
                {
                    Console.WriteLine("Disposing the active context the failed task ran in.");
                    failedTask.GetActiveContext().Value.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("faile to dispose active context {0}", ex.StackTrace);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(ICompletedTask value)
        {
            Console.WriteLine("task completed {0}", value.Id);
            try
            {
                value.ActiveContext.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine("faile to dispose task context {0}", ex.ToString());
            }
        }
    }
}