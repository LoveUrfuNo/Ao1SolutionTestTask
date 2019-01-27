package com.company;

import java.io.*;

import java.math.BigDecimal;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GettingCheapest {
    private static final Integer PRODUCT_ID_FIELD_NUM = 0;

    private static final Integer MAX_NUMBER_OF_SAME_PRODUCTS = 20;
    private static final Integer REQUIRED_NUMBER_OF_CHEAPEST_PRODUCTS = 1000;

    private static final String HEADER = "productId,name,condition,state,price\n";

    private static final String CSV_DELIMITER = ",";

    private static final SortedSet<String> cheapestProducts;
    private static final ConcurrentHashMap<Long, Set<String>> productsLinks;

    static {
        Comparator<String> comparator = (p1, p2) -> {
            final int result = getPrice(p1).compareTo(getPrice(p2));
            return result != 0 ? result : (p1.equals(p2) ? 0 : 1);
        };

        cheapestProducts = new TreeSet<>(comparator);
        productsLinks = new ConcurrentHashMap<>();
    }

    private static BigDecimal getPrice(final String product) {
        final String[] array = product.split(CSV_DELIMITER);
        return new BigDecimal(array[array.length - 1]);
    }

    private static void productsLinksClean() {
        productsLinks.forEach((key, value) -> {
            if (value.size() == 0)
                productsLinks.remove(key);
        });
    }

    private static String getMaxProd(final Set<String> prodLinks) {
        return prodLinks.stream()
                .max(Comparator.comparing(GettingCheapest::getPrice))
                .get();
    }

    private static void removeLastCheapest(final String lastCheapestProd) {
        cheapestProducts.remove(lastCheapestProd);

        final Long productToRemoveId = Long.valueOf(
                lastCheapestProd.split(CSV_DELIMITER)[PRODUCT_ID_FIELD_NUM]);
        final Set<String> links = productsLinks.get(productToRemoveId);
        if (links != null)
            links.remove(lastCheapestProd);
    }

    private static void processProd(final Set<String> prodLinks, final String prod) {
        if (prodLinks.size() < MAX_NUMBER_OF_SAME_PRODUCTS) {
            synchronized (cheapestProducts) {
                if (cheapestProducts.size() >= REQUIRED_NUMBER_OF_CHEAPEST_PRODUCTS) {
                    final String lastCheapestProd =
                            cheapestProducts.isEmpty() ? prod : cheapestProducts.last();
                    if (getPrice(lastCheapestProd).compareTo(getPrice(prod)) < 0) {
                        return;
                    }

                    removeLastCheapest(lastCheapestProd);
                }

                cheapestProducts.add(prod);
                prodLinks.add(prod);
            }
        } else {
            final BigDecimal prodPrice = getPrice(prod);
            final String max = getMaxProd(prodLinks);
            synchronized (cheapestProducts) {
                if (prodPrice.compareTo(getPrice(max)) < 0) {
                    cheapestProducts.remove(max);
                    prodLinks.remove(max);

                    cheapestProducts.add(prod);
                    prodLinks.add(prod);
                }
            }
        }
    }

    private static void searchCheapest(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            reader.readLine();   // skip header
            while (reader.ready()) {
                final String prod = reader.readLine();
                try {
                    final Long prodId = Long.valueOf(
                            prod.split(CSV_DELIMITER)[PRODUCT_ID_FIELD_NUM]);
                    final Set<String> prodLinks =
                            productsLinks.computeIfAbsent(prodId, k -> new HashSet<>());

                    processProd(prodLinks, prod);
                } catch (Throwable t) {
                    System.err.println("Can't process product: " + prod);
                    t.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println(String.format("Error while process %s: %s", file.getName(), e.getMessage()));
            e.printStackTrace();

            return;
        }

        productsLinksClean();

        System.out.println(file.getName() + " was processed");
    }

    private static Params getUserParams() {
        Params params = new Params();
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Hi!\n");
            System.out.print("Enter a csv files dir path: ");
            params.setDirPath(scanner.next());

            System.out.print("Enter a number of threads for files processing: ");
            params.setnThreads(scanner.nextInt());

            System.out.print("Enter a time in minutes how long you can to wait for processing to complete: ");
            params.setWaitingTime(scanner.nextInt());
        }

        return params;
    }

    private static void writeResultToFile() {
        final String resultFilename = "cheapestProducts.csv";
        try (BufferedWriter fWriter = new BufferedWriter(new FileWriter(resultFilename))) {
            fWriter.write(HEADER);
            cheapestProducts.forEach(prod -> {
                try {
                    fWriter.write(prod + "\n");
                } catch (Throwable t) {
                    System.err.println("Couldn't write product on result file: " + prod);
                    t.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Result was written to " + resultFilename);
    }

    public static void main(String[] args) {
        Params params = getUserParams();

        ExecutorService service = Executors.newFixedThreadPool(params.getnThreads());
        try {
            File dir = new File(params.getDirPath());
            Stream.of(Objects.requireNonNull(dir.listFiles()))
                    .forEach(file -> service.execute(() -> searchCheapest(file)));
        } catch (NullPointerException e) {
            System.out.println("Directory doesn't contains files");
            return;
        }

        service.shutdown();
        try {
            service.awaitTermination(params.getWaitingTime(), TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        writeResultToFile();
    }
}