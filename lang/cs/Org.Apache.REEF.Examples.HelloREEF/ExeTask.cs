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
    public sealed class ExeTask : ITask
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(ExeTask));

        [Inject]
        private ExeTask()
        {
        }

        public void Dispose()
        {
            Console.WriteLine("Disposed.");
        }

        public byte[] Call(byte[] memento)
        {
            List<string> arguments = new List<string>();

            File.Copy(Common.GlobalPath + "cabo.exe", "cabo.exe");
            File.Copy(Common.GlobalPath + "cpprest140_2_8.dll", "cpprest140_2_8.dll");
            File.Copy(Common.GlobalPath + Common.GlobalConfigFileName, Common.GlobalConfigFileName);

            foreach (string e in Directory.GetFiles("../"))
            {
                Console.WriteLine("../" + e);
            }

            foreach (string e in Directory.GetFiles("./"))
            {
                Console.WriteLine("./" + e);
            }

            foreach (string e in Directory.GetFiles("./reef"))
            {
                Console.WriteLine("./reef/" + e);
            }

            arguments.Add("dir");
            arguments.Add("--guid");
            arguments.Add("353f35f8-e1c3-461c-8314-5650973a5ec7");
            arguments.Add(Utils.ReadStringArgumentFromConf(Common.GlobalConfigTestEndpoint) + "/" + "download");

            if (!File.Exists(Common.GlobalPath + "cabo.exe"))
            {
                Console.WriteLine("cabo.exe not found");
            }
            if (!File.Exists(Common.GlobalPath + "cpprest140_2_8.dll"))
            {
                Console.WriteLine("cpprest140_2_8.dll not found");
            }

            if (Utils.LaunchJobAsync("cabo.exe", arguments.ToArray()).Result)
            {
                Console.WriteLine("Cabo runner process completed successfully");
            }
            else
            {
                Console.WriteLine("Cabo runner process completed successfully");
            }
            return null;
        }
    }
}