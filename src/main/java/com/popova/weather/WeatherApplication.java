package com.popova.weather;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class WeatherApplication {

    private static final int NUMBER_OF_LINES_IN_HISTORICAL_FILE_HEADER = 7;

    public static void main(String[] args) {
        String historicalFilesSourcePath = getArgument(args, 0).orElse("/data/historical");
        String resultPath = getArgument(args, 1).orElse("/data/results");

        SparkConf conf = new SparkConf().setAppName("Weather Application").setMaster("local[4]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> historicalFileNames = getHistoricalFileNames(historicalFilesSourcePath);

        List<JavaRDD<String>> rawHistoricalDataRDDs = historicalFileNames.stream()
                .map(hfn -> mapToRawHistoricalDataRDD(hfn, sc))
                .collect(Collectors.toList());

        JavaRDD<String> combinedRawHistoricalData = rawHistoricalDataRDDs.stream().reduce(JavaRDD::union).get();

        JavaRDD<MonthlyRecord> monthlyRecordsRDD = combinedRawHistoricalData.map(WeatherApplication::toMonthlyRecord);

        JavaPairRDD<Integer, Iterable<MonthlyRecord>> monthlyRecordsGroupedByMonths = monthlyRecordsRDD
                .groupBy(mr -> mr.getYear() * 100 + mr.getMonth())
                .sortByKey();

        JavaRDD<MonthlyRecord> aggregatedMonthlyRecords = monthlyRecordsGroupedByMonths
                .mapValues(WeatherApplication::groupMonthlyRecords)
                .map(Tuple2::_2);

        JavaRDD<String> aggregatedMonthlyRecordsAsCSVLines = aggregatedMonthlyRecords.map(WeatherApplication::monthlyRecordToCsvLine);


        File resultDirectory = new File(resultPath);
        if (resultDirectory.exists()) delete(resultDirectory);
        aggregatedMonthlyRecordsAsCSVLines.repartition(1).saveAsTextFile("file://"+resultPath);

    }

    private static void delete(File f)  {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                delete(c);
            }
        }
        boolean deletedSuccessfully = f.delete();
        if (!deletedSuccessfully) throw new RuntimeException("Failed to delete file: " + f);
    }

    private static Optional<String> getArgument(String[] args, int i) {
        if(i>args.length-1) return Optional.empty();
        String value = args[i];
        if(value==null || value.trim().isEmpty()) return Optional.empty();
        return Optional.of(value.trim());
    }

    private static String monthlyRecordToCsvLine(MonthlyRecord monthlyRecord) {
        return String.join(",",
                String.valueOf(monthlyRecord.getYear()),
                String.valueOf(monthlyRecord.getMonth()),
                String.valueOf(monthlyRecord.getTemperatureMax()),
                String.valueOf(monthlyRecord.getTemperatureMin()),
                String.valueOf(monthlyRecord.getRain()),
                String.valueOf(monthlyRecord.getAirFrostDays())
        );
    }

    private static MonthlyRecord groupMonthlyRecords(Iterable<MonthlyRecord> monthlyRecordsIterable) {
        List<MonthlyRecord> monthlyRecords = StreamSupport.stream(monthlyRecordsIterable.spliterator(), false).collect(Collectors.toList());
        MonthlyRecord randomRecord = monthlyRecords.iterator().next();

        OptionalDouble averageTemperatureMax = calculateAverageFieldValue(monthlyRecords, MonthlyRecord::getTemperatureMax);
        OptionalDouble averageTemperatureMin = calculateAverageFieldValue(monthlyRecords, MonthlyRecord::getTemperatureMin);
        OptionalDouble averageAirFrostDays = calculateAverageFieldValue(monthlyRecords, MonthlyRecord::getAirFrostDays);
        OptionalDouble averageRain = calculateAverageFieldValue(monthlyRecords, MonthlyRecord::getRain);

        MonthlyRecord result = new MonthlyRecord();
        result.setYear(randomRecord.getYear());
        result.setMonth(randomRecord.getMonth());
        averageTemperatureMax.ifPresent(result::setTemperatureMax);
        averageTemperatureMin.ifPresent(result::setTemperatureMin);
        averageAirFrostDays.ifPresent(avg -> result.setAirFrostDays((int) avg));
        averageRain.ifPresent(result::setRain);
        return result;
    }

    private static <T extends Number> OptionalDouble calculateAverageFieldValue(
            List<MonthlyRecord> monthlyRecords, Function<MonthlyRecord, T> fieldExtractor) {
        return monthlyRecords.stream()
                .filter(monthlyRecord -> fieldExtractor.apply(monthlyRecord)!=null)
                .map(fieldExtractor)
                .map(Number::doubleValue)
                .mapToDouble(x -> x)
                .average();
    }

    private static JavaRDD<String> mapToRawHistoricalDataRDD(String historicalFileName, JavaSparkContext sc) {
        JavaRDD<String> textFileRDD = sc.textFile(historicalFileName);
        JavaPairRDD<String, Long> linesZippedWithIndex = textFileRDD.zipWithIndex();
        JavaPairRDD<String, Long> rawLinesWithIndex = linesZippedWithIndex.filter(lwi -> lwi._2() > NUMBER_OF_LINES_IN_HISTORICAL_FILE_HEADER);
        JavaRDD<String> rawLines = rawLinesWithIndex.map(Tuple2::_1);
        JavaRDD<String> rawLinesWithAdditionalFiltering = rawLines.filter(WeatherApplication::firstMeaningfulTokenIsYear);
        return rawLinesWithAdditionalFiltering.cache();

    }

    private static Boolean firstMeaningfulTokenIsYear(String line) {
        String firstMeaningfulToken = line.trim().split("\\s")[0];
        return firstMeaningfulToken.matches("\\d{4}");
    }

    private static List<String> getHistoricalFileNames(String historicalFilesSourcePath) {
        File dataDirectory = new File(historicalFilesSourcePath);
        File[] historicalFiles = dataDirectory.listFiles();
        return Arrays.stream(historicalFiles)
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
    }

    private static MonthlyRecord toMonthlyRecord(String line) {
        String[] tokens = line.trim().replace("*", "").replace("#", "").split("\\s{1,10}+");
        int year = Integer.parseInt(tokens[0]);
        int month = Integer.parseInt(tokens[1]);
        Double temperatureMax = extractDouble(tokens[2]);
        Double temperatureMin = extractDouble(tokens[3]);
        Integer atFreezeDay = extractInt(tokens[4]);
        Double rain = extractDouble(tokens[5]);

        MonthlyRecord monthlyRecord = new MonthlyRecord();
        monthlyRecord.setYear(year);
        monthlyRecord.setMonth(month);
        monthlyRecord.setTemperatureMax(temperatureMax);
        monthlyRecord.setTemperatureMin(temperatureMin);
        monthlyRecord.setAirFrostDays(atFreezeDay);
        monthlyRecord.setRain(rain);
        return monthlyRecord;
    }

    private static Integer extractInt(String token) {
        if (tokenIsBad(token)) return null;
        return Integer.parseInt(token);
    }

    private static Double extractDouble(String token) {
        if (tokenIsBad(token)) return null;
        return Double.parseDouble(token);
    }

    private static boolean tokenIsBad(String token) {
        return token.isEmpty() || token.equals("---") || token.equals("-");
    }
}
