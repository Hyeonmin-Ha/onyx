/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.compiler.frontend.spark;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.proxy.ClientEndpoint;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;

/**
 * Frontend component for Spark programs.
 */
public class SparkFrontend implements Frontend {
  @Override
  public DAG<IRVertex, IREdge> compile(String className, String[] args) throws Exception {
    // TODO #366: Implement Spark frontend.
    return null;
  }

  @Override
  public ClientEndpoint getClientEndpoint() {
    // TODO #366: Implement Spark frontend.
    return null;
  }
}
