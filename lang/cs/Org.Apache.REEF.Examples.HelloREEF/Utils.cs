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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    class Utils
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Utils));

        public static string ReadStringArgumentFromConf(string argumentName, string fileName = Common.GlobalConfigFileName)
        {
            var arguments = string.Join(";", File.ReadAllLines(fileName));
            return ExtractStringArgument(arguments, argumentName);
        }

        public static int ReadIntArgumentFromConf(string argumentName, string fileName = Common.GlobalConfigFileName)
        {
            var arguments = string.Join(";", File.ReadAllLines(fileName));
            return ExtractIntArgument(arguments, argumentName);
        }

        public static string ExtractStringArgument(string arguments, string argumentName)
        {
            var conf = arguments.Split(';').ToDictionary(x => x.Split('=').First(), x => x.Split('=').Last());
            return conf[argumentName];
        }

        public static int ExtractIntArgument(string arguments, string argumentName)
        {
            var conf = arguments.Split(';').ToDictionary(x => x.Split('=').First(), x => x.Split('=').Last());
            return int.Parse(conf[argumentName]);
        }

        /// <summary>
        /// Launch a java class in ClientConstants.ClientJarFilePrefix with provided parameters.
        /// </summary>
        public static Task<bool> LaunchJobAsync(string fileName, string[] arguments)
        {
            var startInfo = new ProcessStartInfo
            {
                Arguments = string.Join(" ", arguments),
                FileName = fileName,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            var msg = string.Format(CultureInfo.CurrentCulture, "Launch Java with command: {0} {1}",
                startInfo.FileName, startInfo.Arguments);
            Logger.Log(Level.Info, msg);

            var process = Process.Start(startInfo);
            var processExitTracker = new TaskCompletionSource<bool>();
            if (process != null)
            {
                process.EnableRaisingEvents = true;
                process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
                {
                    if (!string.IsNullOrWhiteSpace(e.Data))
                    {
                        Logger.Log(Level.Info, e.Data);
                    }
                };
                process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
                {
                    if (!string.IsNullOrWhiteSpace(e.Data))
                    {
                        Logger.Log(Level.Error, e.Data);
                    }
                };
                process.BeginErrorReadLine();
                process.BeginOutputReadLine();
                process.Exited += (sender, args) => { processExitTracker.SetResult(process.ExitCode == 0); };
            }
            else
            {
                processExitTracker.SetException(new Exception("Java client process didn't start."));
            }

            return processExitTracker.Task;
        }
    }
}
