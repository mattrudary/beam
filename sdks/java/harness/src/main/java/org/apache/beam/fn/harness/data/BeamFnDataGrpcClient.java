/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.fn.harness.data;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataBufferingOutboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer2;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that uses gRPC for sending and receiving data.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnDataGrpcClient implements BeamFnDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);

  private final ConcurrentMap<Endpoints.ApiServiceDescriptor, BeamFnDataGrpcMultiplexer2> cache;
  private final Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory;
  private final OutboundObserverFactory outboundObserverFactory;
  private final PipelineOptions options;

  public BeamFnDataGrpcClient(
      PipelineOptions options,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      OutboundObserverFactory outboundObserverFactory) {
    this.options = options;
    this.channelFactory = channelFactory;
    this.outboundObserverFactory = outboundObserverFactory;
    this.cache = new ConcurrentHashMap<>();
  }

  @Override
  public void registerReceiver(
      String instructionId,
      List<ApiServiceDescriptor> apiServiceDescriptors,
      CloseableFnDataReceiver<Elements> receiver) {
    LOG.debug("Registering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer2 client = getClientFor(apiServiceDescriptors.get(i));
      client.registerConsumer(instructionId, receiver);
    }
  }

  @Override
  public void unregisterReceiver(
      String instructionId, List<ApiServiceDescriptor> apiServiceDescriptors) {
    LOG.debug("Unregistering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer2 client = getClientFor(apiServiceDescriptors.get(i));
      client.unregisterConsumer(instructionId);
    }
  }

  /**
   * Creates a closeable consumer using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>On closing the returned consumer, an empty data block is sent as a signal of the logical
   * data stream finishing.
   *
   * <p>The returned closeable consumer is not thread safe.
   */
  @Override
  public <T> CloseableFnDataReceiver<T> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<T> coder) {
    BeamFnDataGrpcMultiplexer2 client = getClientFor(apiServiceDescriptor);

    LOG.debug("Creating output consumer for {}", outputLocation);
    return BeamFnDataBufferingOutboundObserver.forLocation(
        options, outputLocation, coder, client.getOutboundObserver());
  }

  private BeamFnDataGrpcMultiplexer2 getClientFor(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor) {
    return cache.computeIfAbsent(
        apiServiceDescriptor,
        (Endpoints.ApiServiceDescriptor descriptor) ->
            new BeamFnDataGrpcMultiplexer2(
                descriptor,
                outboundObserverFactory,
                BeamFnDataGrpc.newStub(channelFactory.apply(apiServiceDescriptor))::data));
  }
}
