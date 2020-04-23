#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

from __future__ import absolute_import

import unittest
from builtins import object

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam import metrics
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import counters


class NameTest(unittest.TestCase):
  def test_basic_metric_name(self):
    name = MetricName('namespace1', 'name1')
    self.assertEqual(name.namespace, 'namespace1')
    self.assertEqual(name.name, 'name1')
    self.assertEqual(name, MetricName('namespace1', 'name1'))

    key = MetricKey('step1', name)
    self.assertEqual(key.step, 'step1')
    self.assertEqual(key.metric.namespace, 'namespace1')
    self.assertEqual(key.metric.name, 'name1')
    self.assertEqual(key, MetricKey('step1', MetricName('namespace1', 'name1')))


class MetricResultsTest(unittest.TestCase):
  def test_metric_filter_namespace_matching(self):
    filter = MetricsFilter().with_namespace('ns1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

  def test_metric_filter_name_matching(self):
    filter = MetricsFilter().with_name('name1').with_namespace('ns1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_name('name1')
    name = MetricName('ns1', 'name1')
    key = MetricKey('step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

  def test_metric_filter_step_matching(self):
    name = MetricName('ns1', 'name1')
    filter = MetricsFilter().with_step('Step1')

    key = MetricKey('Step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    key = MetricKey('Step10', name)
    self.assertFalse(MetricResults.matches(filter, key))

    key = MetricKey('Step10/Step1', name)
    self.assertTrue(MetricResults.matches(filter, key))

    key = MetricKey('Top1/Outer1/Inner1', name)

    filter = MetricsFilter().with_step('Top1/Outer1/Inner1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Outer1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Outer1/Inner1')
    self.assertTrue(MetricResults.matches(filter, key))

    filter = MetricsFilter().with_step('Top1/Inner1')
    self.assertFalse(MetricResults.matches(filter, key))


class MetricsTest(unittest.TestCase):
  def test_get_namespace_class(self):
    class MyClass(object):
      pass

    self.assertEqual(
        '{}.{}'.format(MyClass.__module__, MyClass.__name__),
        Metrics.get_namespace(MyClass))

  def test_get_namespace_string(self):
    namespace = 'MyNamespace'
    self.assertEqual(namespace, Metrics.get_namespace(namespace))

  def test_get_namespace_error(self):
    with self.assertRaises(ValueError):
      Metrics.get_namespace(object())

  def test_counter_empty_name(self):
    with self.assertRaises(ValueError):
      Metrics.counter("namespace", "")

  def test_counter_empty_namespace(self):
    with self.assertRaises(ValueError):
      Metrics.counter("", "names")

  def test_distribution_empty_name(self):
    with self.assertRaises(ValueError):
      Metrics.distribution("namespace", "")

  def test_distribution_empty_namespace(self):
    with self.assertRaises(ValueError):
      Metrics.distribution("", "names")

  @attr('ValidatesRunner')
  def test_user_counter_using_pardo(self):
    class SomeDoFn(beam.DoFn):
      """A custom dummy DoFn using yield."""
      def __init__(self):
        self.user_counter_elements = metrics.Metrics.counter(
            self.__class__, 'metrics_user_counter_element')

      def process(self, element):
        self.user_counter_elements.inc()
        yield element

    pipeline = TestPipeline()
    nums = pipeline | 'Input' >> beam.Create([1, 2, 3, 4])
    results = nums | 'ApplyPardo' >> beam.ParDo(SomeDoFn())
    assert_that(results, equal_to([1, 2, 3, 4]))

    res = pipeline.run()
    res.wait_until_finish()
    metric_results = (
        res.metrics().query(
            MetricsFilter().with_name('metrics_user_counter_element')))
    outputs_counter = metric_results['counters'][0]

    self.assertEqual(
        outputs_counter.key.metric.name, 'metrics_user_counter_element')
    self.assertEqual(outputs_counter.committed, 4)

  def test_create_counter_distribution(self):
    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state1 = sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

    try:
      sampler.start()
      with state1:
        counter_ns = 'aCounterNamespace'
        distro_ns = 'aDistributionNamespace'
        name = 'a_name'
        counter = Metrics.counter(counter_ns, name)
        distro = Metrics.distribution(distro_ns, name)
        counter.inc(10)
        counter.dec(3)
        distro.update(10)
        distro.update(2)
        self.assertTrue(isinstance(counter, Metrics.DelegatingCounter))
        self.assertTrue(isinstance(distro, Metrics.DelegatingDistribution))

        del distro
        del counter

        container = MetricsEnvironment.current_container()
        self.assertEqual(
            container.get_counter(MetricName(counter_ns,
                                             name)).get_cumulative(),
            7)
        self.assertEqual(
            container.get_distribution(MetricName(distro_ns,
                                                  name)).get_cumulative(),
            DistributionData(12, 2, 2, 10))
    finally:
      sampler.stop()


if __name__ == '__main__':
  unittest.main()