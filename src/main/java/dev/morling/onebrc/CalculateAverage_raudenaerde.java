/*
 *  Copyright 2023 Rob Audenaerde
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_raudenaerde {

    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, int value) {
        public static Measurement create(String line) {
            int index = line.indexOf(';');
            String station = line.substring(0, index);
            String value = line.substring(index + 1);
            int valInt = (int) (10 * Double.parseDouble(value));
            return new Measurement(station, valInt);
        }
    }

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private int count;

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((double) sum / (double) count) + "/" + round(max);
        }
    }

    public static void main(String[] args) throws IOException {

        Collector<Measurement, ?, ConcurrentMap<String, MeasurementAggregator>> measurementConcurrentMapCollector = Collectors.groupingByConcurrent(Measurement::station,
                Collector.of(
                        MeasurementAggregator::new,
                        (a, m) -> {
                            a.min = Math.min(a.min, m.value());
                            a.max = Math.max(a.max, m.value());
                            a.sum += m.value();
                            a.count++;
                        },
                        (agg1, agg2) -> {
                            var res = new MeasurementAggregator();
                            res.min = Math.min(agg1.min, agg2.min);
                            res.max = Math.max(agg1.max, agg2.max);
                            res.sum = agg1.sum + agg2.sum;
                            res.count = agg1.count + agg2.count;
                            return res;
                        }));

        ConcurrentMap<String, MeasurementAggregator> result = Files.lines(Paths.get(FILE)).parallel().map(Measurement::create)
                .collect(measurementConcurrentMapCollector);

        Map<String, MeasurementAggregator> treeMap = new TreeMap<>(result);
        System.out.println(treeMap);
    }
}
