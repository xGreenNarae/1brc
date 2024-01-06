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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_xGreenNarae {

    private static final String FILE = "./measurements.txt";

    @SuppressWarnings("preview")
    public static void main(String[] args) throws IOException {
        final File file = new File(FILE);
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        final FileChannel fileChannel = randomAccessFile.getChannel();
        final MemorySegment memSeg = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

        final TreeMap<String, ResultRow> results = (((divideFileSegments(memSeg).stream().parallel().map(segment -> {
            Map<String, MeasurementAggregator> resultMap = new HashMap<>();
            final long segmentEnd = segment.end();

            MemorySegment slice = memSeg.asSlice(segment.start(), segmentEnd - segment.start());

            for (int offset = 0; offset < slice.byteSize();) {
                if (slice.get(ValueLayout.JAVA_BYTE, offset) == '\n') {
                    byte[] bytes = new byte[offset];
                    for (int i = 0; i < offset; i++) {
                        bytes[i] = slice.get(ValueLayout.JAVA_BYTE, i);
                    }
                    String line = new String(bytes, StandardCharsets.UTF_8);
                    String[] parts = line.split(";");
                    String station = parts[0];
                    double value = Double.parseDouble(parts[1]);

                    MeasurementAggregator aggregator = (MeasurementAggregator) resultMap.computeIfAbsent(station, k -> new MeasurementAggregator());
                    aggregator.min = Math.min(aggregator.min, value);
                    aggregator.max = Math.max(aggregator.max, value);
                    aggregator.sum += value;
                    aggregator.count++;

                    offset++;
                    slice = slice.asSlice(offset, slice.byteSize() - offset);
                    offset = 0;
                }
                else {
                    offset++;
                }
            }

            return resultMap;
        }).reduce(new HashMap<String, MeasurementAggregator>(), (finalMap, segmentMap) -> {
            segmentMap.forEach((station, aggregator) -> {
                finalMap.merge(station, aggregator, CalculateAverage_xGreenNarae::merge);
            });
            return finalMap;
        }).entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new ResultRow(
                                entry.getValue().min,
                                entry.getValue().sum / entry.getValue().count,
                                entry.getValue().max),
                        (e1, e2) -> e1, TreeMap::new)))));

        System.out.println(results);
    }

    private static MeasurementAggregator merge(MeasurementAggregator existingValue, MeasurementAggregator newValue) {
        existingValue.min = Math.min(existingValue.min, newValue.min);
        existingValue.max = Math.max(existingValue.max, newValue.max);
        existingValue.sum += newValue.sum;
        existingValue.count += newValue.count;

        return existingValue;
    }

    /**
     * 사용 가능한 CPU 프로세서 수에 맞춰 파일을 블록 단위로 나눕니다.
     */
    private static List<FileSegment> divideFileSegments(MemorySegment memSeg) {
        final int numOfCpus = Runtime.getRuntime().availableProcessors();
        final long fileSize = memSeg.byteSize();
        final long segmentSize = fileSize / numOfCpus;
        List<FileSegment> fileSegments = new ArrayList<>(numOfCpus);

        for (int i = 0; i < numOfCpus; i++) {
            long start = i * segmentSize;
            long end = start + segmentSize;
            if (i == numOfCpus - 1) {
                end = fileSize;
            }

            start = findSegment(i, 0, memSeg, start, fileSize);
            end = findSegment(i, numOfCpus - 1, memSeg, end, fileSize);

            fileSegments.add(new FileSegment(start, end));
        }

        return fileSegments;
    }

    /**
     * 줄바꿈 문자의 경계에 세그먼트를 맞춥니다.
     */
    private static long findSegment(int i, int skipSegment, MemorySegment memSeg, long location, long fileSize) {
        if (i != skipSegment) {
            long remaining = fileSize - location;
            int bufferSize = 16;
            if (remaining < bufferSize) {
                bufferSize = (int) remaining;
            }

            MemorySegment slice = memSeg.asSlice(location, bufferSize);

            for (int offset = 0; offset < bufferSize; offset++) {
                if (slice.get(ValueLayout.JAVA_BYTE, offset) == '\n') {
                    return location + offset + 1;
                }
            }
        }
        return location;
    }

    private static record FileSegment(long start, long end) {
    }

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }

    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }
}
