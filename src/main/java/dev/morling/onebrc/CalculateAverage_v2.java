/*
 *  Copyright 2023 The original authors
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.stream.Collectors.*;

public class CalculateAverage_v2 {
    public static final int BUFFER_SIZE = 8192;

    private static record MeasurementsResult(double min, double mean, double max) {
		public String toString() {
			return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
		}

		private double round(double value) {
			return Math.round(value * 10.0) / 10.0;
		}
	}

    private static class ParsedMeasurements {
        private final String name;
        private final String value;

        public ParsedMeasurements(String row) {
            int i = row.indexOf(';');
            this.name = row.substring(0, i);
            this.value = row.substring(i + 1);
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }
    }

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE), BUFFER_SIZE)) {
            Map<String, List<Double>> parsed = reader.lines()
                    .map(ParsedMeasurements::new)
                    .collect(groupingBy(ParsedMeasurements::getName, mapping(parsedMeasurements -> Double.parseDouble(parsedMeasurements.getValue()), toList())));

            Map<String, MeasurementsResult> aggregatedResult = parsed.entrySet().stream().parallel().collect(toMap(Map.Entry::getKey, entry -> {
                List<Double> values = entry.getValue();

                return new MeasurementsResult(
                        values.stream().min(Double::compare).get(),
                        (Math.round(values.stream().reduce(0.0, Double::sum) * 10.0) / 10.0) / values.size(),
                        values.stream().max(Double::compare).get());
            }));

            System.out.println(new TreeMap<>(aggregatedResult));
        }
    }
}
