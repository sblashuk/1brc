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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_sblashuk {

    public static final String SPLIT_SYMBOL = ";";
    public static final int BUFFER_SIZE = 8192;

    private record Measurements(double min, double max, double sum, long count) {
		public String toString() {
				return STR."\{round(min)}/\{round((Math.round(sum * 10.0) / 10.0) / count)}/\{round(max)}";
			}

			private double round(double value) {
				return Math.round(value * 10.0) / 10.0;
			}
		}

    private static class ParsedMeasurements {
        private final String name;
        private final Double value;

        public ParsedMeasurements(String row) {
            int i = row.indexOf(';');
            this.name = row.substring(0, i);
            String valueStr = row.substring(i + 1);
            this.value = Double.parseDouble(valueStr);
        }

        public String getName() {
            return name;
        }

        public Double getValue() {
            return value;
        }
    }

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, Measurements> measurementsAggregation = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(FILE), BUFFER_SIZE)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ParsedMeasurements parsedMeasurements = new ParsedMeasurements(line);

                measurementsAggregation.compute(parsedMeasurements.getName(),
                        (key, existingMeasurements) -> aggregate(parsedMeasurements, existingMeasurements));
            }
        }
        System.out.println(new TreeMap<>(measurementsAggregation));
    }

    public static Measurements aggregate(ParsedMeasurements parsedMeasurements, Measurements existingMeasurements) {
        if (existingMeasurements == null) {
            return new Measurements(Math.min(Double.POSITIVE_INFINITY, parsedMeasurements.getValue()),
                    Math.max(Double.NEGATIVE_INFINITY, parsedMeasurements.getValue()), parsedMeasurements.getValue(), 1);
        }
        return new Measurements(Math.min(existingMeasurements.min, parsedMeasurements.getValue()),
                Math.max(existingMeasurements.max, parsedMeasurements.getValue()), existingMeasurements.sum() + parsedMeasurements.getValue(),
                existingMeasurements.count() + 1);
    }
}
