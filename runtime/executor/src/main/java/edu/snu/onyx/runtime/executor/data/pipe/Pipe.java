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
package edu.snu.onyx.runtime.executor.data.pipe;

import edu.snu.onyx.runtime.executor.data.NonSerializedElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class Pipe {
  private static final Logger LOG = LoggerFactory.getLogger(Pipe.class.getName());
  private LinkedBlockingQueue<NonSerializedElement> pipe;

  public Pipe() {
    this.pipe = new LinkedBlockingQueue<>();
  }

  /**
   * Put {@link NonSerializedElement} into this pipe.
   *
   * @param elementToPut the {@link NonSerializedElement} to put.
   */
  public synchronized void putElement(final NonSerializedElement elementToPut) {
    try {
      pipe.put(elementToPut);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while pipe.putElement()", e);
    }
  }

  /**
   * Retrieves the {@link NonSerializedElement}.
   *
   * @return a {@link NonSerializedElement}.
   */
  public NonSerializedElement getElement() {
    return pipe.poll();
  }
}
