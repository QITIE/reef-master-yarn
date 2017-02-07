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
using System.Collections;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.IO.FileSystem.Hadoop;
using Org.Apache.REEF.IO.FileSystem;
using System.Text;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using Org.Apache.REEF.Client.API.Exceptions;
using Org.Apache.REEF.Utilities.Diagnostics;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class JavaTask : ITask
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(JavaTask));

        [Inject]
        private JavaTask()
        {
        }

        public void Dispose()
        {
            Console.WriteLine("Disposed.");
        }

        public byte[] Call(byte[] memento)
        {
            PrepareRunFiles();
            var envs = Environment.GetEnvironmentVariables();
            PrintSystemInfomation(envs);
            var arguments = GetArguments(envs);
            var javaCommand = GetJavaCommand(envs);
            PrintDebugInfo(envs, arguments);
            if (Utils.LaunchJobAsync(javaCommand, arguments.ToArray()).Result)
            {
                Console.WriteLine("Java runner process completed successfully");
            }
            else
            {
                Console.WriteLine("Java runner process completed successfully");
            }
            return null;
        }

        private static void PrintDebugInfo(IDictionary envs, List<string> arguments)
        {
            Console.WriteLine("command :" + (string)envs["JAVA"] + " " + string.Join(" ", arguments));
            Console.WriteLine((string)envs["JAVA"] + ".exe" + " exists " + File.Exists((string)envs["JAVA"] + ".exe"));
            Console.WriteLine("reef-bridge-client-0.16.0-SNAPSHOT-shaded.jar exists " +
                              File.Exists("reef/global/reef-bridge-client-0.16.0-SNAPSHOT-shaded.jar"));
            Console.WriteLine("endpoint exists " + File.Exists("reef/global/endpoint.txt"));

            Console.WriteLine(Directory.GetFiles(Environment.CurrentDirectory, "*.*", SearchOption.AllDirectories));
            Console.WriteLine(Environment.CommandLine);
            Console.WriteLine(Environment.CurrentDirectory);
        }

        private static List<string> GetArguments(IDictionary envs)
        {
            List<string> arguments = new List<string>();

            arguments.Add("-cp");
            arguments.Add(".\\*;" + (string)envs["CLASSPATH"]);
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigJavaRunnerClassName));
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigTestEndpoint));
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigBlockSizeMB)); // blocksizeMB
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigTotalClientIO)); // total upload MB
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigClientThreads));
            arguments.Add((string)envs["REEF_YARN_APPLICATION_ID"]);
            return arguments;
        }

        private static string GetJavaCommand(IDictionary envs)
        {
            string javaCommand = (string)envs["JAVA"] + ".exe";
            return javaCommand;
        }

        private static void PrintSystemInfomation(IDictionary envs)
        {
            foreach (var k in envs.Keys)
            {
                Console.WriteLine("Enviornment Var >>>>>>:" + k + ":" + envs[k]);
            }
        }

        private static void PrepareRunFiles()
        {
            File.Copy(Common.GlobalPath + Common.GlobalConfigFileName,
                Common.GlobalConfigFileName);
            File.Copy(Common.GlobalPath + Common.ReefJavaClientJarFileName,
                Common.ReefJavaClientJarFileName);
        }
    }
}