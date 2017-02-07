/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tests.evaluator.failure;

import org.apache.commons.lang3.Validate;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.poison.PoisonedConfiguration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tests.evaluator.failure.parameters.NumEvaluatorsToFail;
import org.apache.reef.tests.evaluator.failure.parameters.NumEvaluatorsToSubmit;
import org.apache.reef.tests.library.exceptions.DriverSideFailure;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver for failure test.
 */
@Unit
public class FailureDriver {

  private final int numEvaluatorsToSubmit;
  private final int numEvaluatorsToFail;
  private final AtomicInteger numEvaluatorsLeftToSubmit;
  private final AtomicInteger numEvaluatorsLeftToClose;
  private static final Logger LOG = Logger.getLogger(FailureDriver.class.getName());
  private final EvaluatorRequestor requestor;

  @Inject
  public FailureDriver(@Parameter(NumEvaluatorsToSubmit.class) final int numEvaluatorsToSubmit,
                       @Parameter(NumEvaluatorsToFail.class) final int numEvaluatorsToFail,
                       final EvaluatorRequestor requestor) {
    Validate.isTrue(numEvaluatorsToSubmit > 0, "The number of Evaluators to submit must be greater than 0.");
    Validate.inclusiveBetween(1, numEvaluatorsToSubmit, numEvaluatorsToFail,
        "The number of Evaluators to fail must be between 1 and numEvaluatorsToSubmit, inclusive.");

    this.numEvaluatorsToSubmit = numEvaluatorsToSubmit;
    this.numEvaluatorsToFail = numEvaluatorsToFail;

    this.numEvaluatorsLeftToSubmit = new AtomicInteger(numEvaluatorsToSubmit);

    // We should close numEvaluatorsToSubmit because all failed Evaluators are eventually resubmitted and closed.
    this.numEvaluatorsLeftToClose = new AtomicInteger(numEvaluatorsToSubmit);

    this.requestor = requestor;
    LOG.info("Driver instantiated");
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.FINE, "Request {0} Evaluators.", numEvaluatorsToSubmit);
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numEvaluatorsToSubmit)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit a poisoned context.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINE, "Got allocated evaluator: {0}", evalId);
      if (numEvaluatorsLeftToSubmit.getAndDecrement() > 0) {
        LOG.log(Level.FINE, "Submitting poisoned context. {0} to go.", numEvaluatorsLeftToSubmit);
        allocatedEvaluator.submitContext(
            Tang.Factory.getTang()
                .newConfigurationBuilder(
                    ContextConfiguration.CONF
                        .set(ContextConfiguration.IDENTIFIER, "Poisoned Context: " + evalId)
                        .build(),
                    PoisonedConfiguration.CONTEXT_CONF
                        .set(PoisonedConfiguration.CRASH_PROBABILITY, "1")
                        .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
                        .build())
                .build());
      } else {
        LOG.log(Level.FINE, "Closing evaluator {0}", evalId);
        allocatedEvaluator.close();
        FailureDriver.this.numEvaluatorsLeftToClose.decrementAndGet();
      }
    }
  }

  /**
   * Handles FailedEvaluator: Resubmits the single Evaluator resource request.
   */
  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.FINE, "Got failed evaluator: {0} - re-request", failedEvaluator.getId());
      FailureDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Checks whether all failed Evaluators were properly resubmitted and restarted.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      final int numEvaluatorsToClose = FailureDriver.this.numEvaluatorsLeftToClose.get();
      if (numEvaluatorsToClose != 0){
        final String message = "Got RuntimeStop Event. Expected to close " + numEvaluatorsToSubmit + " Evaluators " +
            "but only " + (numEvaluatorsToSubmit - numEvaluatorsToClose) + " Evaluators were closed.";
        LOG.log(Level.SEVERE, message);
        throw new DriverSideFailure(message);
      }
    }
  }
}
