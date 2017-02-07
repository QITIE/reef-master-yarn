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
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using System.IO;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Client.Yarn.RestClient;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution.
    /// </summary>
    public sealed class LoadGenerator
    {
        // this endpoint is used for coordinating load generation job.
        private static string endpoint = "adl://kobo03tsperfmain.caboaccountdogfood.net/";

        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private LoadGenerator(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<Driver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<Driver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<Driver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<Driver>.Class)                
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobRequest = _jobRequestBuilder
                .AddDriverConfiguration(helloDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(Driver))
                .AddGlobalFile(Common.GlobalConfigFileName)
                .SetJobIdentifier(Utils.ReadStringArgumentFromConf(Common.GlobalConfigTaskName))
                .Build();

            _reefClient.Submit(helloJobRequest);
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration()
        {
            string token = File.ReadAllText(@"tokens.txt");
            var data = Convert.FromBase64String(token);
            File.WriteAllBytes("SecurityTokenId", data);
            File.WriteAllText("SecurityTokenPwd", "none");

            string token2 = "TrustedApplication007";
            File.WriteAllText("SecurityTokenId2", token2);
            File.WriteAllText("SecurityTokenPwd2", "none");

            IConfiguration tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
            .Set(TcpPortConfigurationModule.PortRangeStart, "2000")
            .Set(TcpPortConfigurationModule.PortRangeCount, "50")
            .Build();

            var c = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.JobSubmissionFolderPrefix, endpoint + "/tmp/")
                .Set(YARNClientConfiguration.SecurityTokenKind, "CaboUserSecurityKeyToken")
                .Set(YARNClientConfiguration.SecurityTokenService, "CaboUserSecurityKeyToken")
                .Build();

            var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IUrlProvider>.Class, GenericType<YarnConfigurationUrlProvider>.Class)
                .Build();

            return Configurations.Merge(c, tcpPortConfig, c2);
        }

        public static void Main(string[] args)
        {
            if (File.Exists("LoadGenerator.ini"))
            {
                File.Delete("LoadGenerator.ini");
            }
            File.Copy(args[0], "LoadGenerator.ini");
            TangFactory.GetTang().NewInjector(GetRuntimeConfiguration()).GetInstance<LoadGenerator>().Run();
        }
    }
}