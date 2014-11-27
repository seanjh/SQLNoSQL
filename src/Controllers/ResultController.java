package Controllers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ResultController {
    public static void printResult(Map<String, Object> result) {
        String key;
        String value;
        for(Entry<String, Object> entry : result.entrySet()) {
            if (entry.getValue() != null) {
                key = entry.getKey();
                value = entry.getValue().toString();
                System.out.printf("\t%s: %s\n", key, value);
            }
        }
    }

    public static void printResult(Iterator<Map<String, Object>> results) {
        int i = 0;
        Map<String, Object> result;
        while (results.hasNext()) {
            result = results.next();
            printResult(result);
            i++;
        }
        if (i == 0) {
            System.out.printf("\tEmpty set\n");
        }
    }

    public static long printStart(String queryDesc) {
        System.out.println();
        System.out.printf("Beginning Query: %s\n", queryDesc);
        return System.nanoTime();
    }

    public static long printStart(String queryDesc, String queryKey, String queryValue) {
        System.out.println();
        System.out.printf("Beginning Query: %s (%s = %s)\n", queryDesc, queryKey, queryValue);
        return System.nanoTime();
    }

    public static long printEnd(long startTime) {
        long elapsed = System.nanoTime() - startTime;
        System.out.printf("Completed Query in %.3f seconds\n", (float) elapsed/Math.pow(10,9));
        return elapsed;
    }

    public static void printElapsedReview(List<Long> elapsed, String listDesc) {
        long max = elapsed.get(0);
        long min = elapsed.get(0);
        long sum = elapsed.get(0);
        for (int i=1; i<elapsed.size(); i++) {
            if (elapsed.get(i) > max)
                max = elapsed.get(i);
            if (elapsed.get(i) < min)
                min = elapsed.get(i);
            sum += elapsed.get(i);
        }

        System.out.printf("%s\n\tMax:%.3f seconds\n\tMin:%.3f seconds\n\tMean:%.3f seconds\n", listDesc,
                (float) max/Math.pow(10,9),
                (float) min/Math.pow(10,9),
                ((float) sum / elapsed.size())/Math.pow(10,9)
                );
    }
}
