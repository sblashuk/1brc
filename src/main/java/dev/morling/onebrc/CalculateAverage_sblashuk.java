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
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_sblashuk {

    public static final int BUFFER_SIZE = 8192;
    public static final char SPLIT = ';';

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
            int i = row.indexOf(SPLIT);
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

    private static class BufferedRandomAccessFileReader implements Closeable {
        private final RandomAccessFile file;
        private final Long fileEnd;
        private Long filePos;

        private BufferedReader bufferedReader;

        public BufferedRandomAccessFileReader(Long start, Long end) throws IOException {
            this.file = new RandomAccessFile(FILE, "r");
            this.file.seek(start);
            this.filePos = start;
            this.fileEnd = end;
            this.bufferedReader = prepareNextBuffer();
        }

        public String readLine() throws IOException {
            String row = this.bufferedReader.readLine();

            if (row == null && filePos < fileEnd) {
                bufferedReader = prepareNextBuffer();
                row = this.bufferedReader.readLine();
            }
            else if (row == null) {
                row = null;
            }

            return row;
        }

        private BufferedReader prepareNextBuffer() throws IOException {
            long i = Math.min(filePos + BUFFER_SIZE + 1, file.length());

            int value;
            do {
                i--;
                file.seek(i);
                value = file.read();
            } while (value != 10 && value != -1);

            byte[] buffer = new byte[(int) (i - filePos)];
            file.seek(filePos);
            file.read(buffer);

            filePos = i + 1;
            return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
        }

        @Override
        public void close() throws IOException {
            this.file.close();
        }
    }

    private static final String FILE = "./measurements.txt";
    private static final Map<String, Measurements> AGGREGATION = new ConcurrentHashMap<>();
    private static final Integer CORES = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException, InterruptedException {
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
            long chunks = (int) (file.length() / CORES);

            int numberOfThreads = CORES;

            if (chunks < BUFFER_SIZE) {
                numberOfThreads = 1;
                chunks = file.length();
            }

            CountDownLatch latch;
            try (ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads)) {
                latch = new CountDownLatch(numberOfThreads);

                for (long i = 0, startPos = 0, endPos = 0; i < numberOfThreads; i++) {
                    file.seek((i + 1) * chunks);

                    endPos = (i + 1) * chunks;
                    int value = file.read();
                    while (value != 10 && value != -1) {
                        endPos++;
                        value = file.read();
                    }

                    final long start = startPos;
                    final long end = endPos;
                    executor.execute(() -> {
                        try (BufferedRandomAccessFileReader reader = new BufferedRandomAccessFileReader(start, end)) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                ParsedMeasurements parsedMeasurements = new ParsedMeasurements(line);

                                AGGREGATION.compute(parsedMeasurements.getName(),
                                        (key, existingMeasurements) -> aggregate(parsedMeasurements, existingMeasurements));
                            }

                        }
                        catch (IOException e) {
                            latch.countDown();
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                    startPos = endPos + 1;
                }
            }

            latch.await();
            System.out.println(new TreeMap<>(AGGREGATION));
        }
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
