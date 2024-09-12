// Assignment 1 of Concurrent Programming(CSC 2044)
// Student ID: 20055810

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

// A read-only class representing each product
// A product has a name (Product A, Product B, etc.)
// and Its profit
class Product {
    private String productName;
    private double profit;
    // Constructor to initialize product name and profit
    public Product(String productName, double profit) {
        this.productName = productName;
        this.profit = profit;
    }

    // Getter for product name
    public String getProductName() {
        return productName;
    }
    
    // Getter for profit
    public double getProfit() {
        return profit;
    }

    // Overriding toString method for meaningful representation
    @Override
    public String toString() {
        return "Info {name='" + productName + "', profit=" + profit + "}";
    }
}

// Class representing a Branch and its related data and methods
class Branch {
    private String branchID;
    private double profit;

    public Branch(String branchID) {
        this.branchID = branchID;
        this.profit = 0.0;
    }

    public String getBranchID() {
        return branchID;
    }

    public double getProfit() {
        return profit;
    }

    public void addProfit(double profit) {
        this.profit += profit;
    }

    @Override
    public String toString() {
        return "Branch{id='" + branchID + "', profit=" + profit + "}";
    }
}


public class Main {
    private static final int NUM_THREADS = 4; // Number of threads to use

    public static void main(String[] args) {
        String csvFile = "C:\\Users\\Shanthi Saravanan\\OneDrive - Sunway Education Group\\Desktop\\CSC2044_Assignment 1 Submission_20055810\\sales_records (1)_int.csv"; // Specifying the path to CSV file of the sales data
        Map<String, Map<String, Integer>> salesData = readCSV(csvFile); // Read sales data from CSV file
        if (salesData == null) {
            System.out.println("Failed to read CSV file.");            // To check if reading the CSV file failed
            return;
        }

        // Product profits
        Map<String, Product> products = createProducts();  // Creates a Map of Product objects with predefined profit values for each product

        Map<String, Integer> totalUnitsSold = TotalUnitsSold_Calculator(salesData);    // To calculate total units sold for each product

        // Maps to store total profits for products and branches
        Map<String, Double> totalProductProfits = new ConcurrentHashMap<>();
        Map<String, Branch> branchProfits = new ConcurrentHashMap<>();
        double totalDailyProfits = TotalDailyProfits_Calculator(salesData, products, totalProductProfits, branchProfits);

        // To find the branch with the lowest profit
        Branch branchWithLowestProfit = findBranchWithLowestProfit(branchProfits);

        // Output results
        // for Total Units Sold
        System.out.println("------------------------------------------------");
        System.out.println("Total Units Sold:");
        for (Map.Entry<String, Integer> entry : totalUnitsSold.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        // for Total Daily Profits across all the branches present in the list
        System.out.println("------------------------------------------------");
        System.out.println("Total Daily Profits: " + "$" + totalDailyProfits);
        System.out.println("------------------------------------------------");
        System.out.println("Total Profit for Each Product from all Branch:");
        for (Map.Entry<String, Double> entry : totalProductProfits.entrySet()) {
            System.out.println(entry.getKey() + ": " + "$" + entry.getValue());
        }
        // 1 branch out of all branches in the list with the lowest profit
        System.out.println("------------------------------------------------");
        System.out.println("Branch with Lowest Profit: " + "Branch No. " + branchWithLowestProfit.getBranchID() + ", Profit: $" + branchWithLowestProfit.getProfit());
        System.out.println("------------------------------------------------");
    }

    // Method to read CSV file and return sales data
    private static Map<String, Map<String, Integer>> readCSV(String csvFile) {
        Map<String, Map<String, Integer>> salesData = new HashMap<>();
        String line;
        String delimiter = ",";
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String header = br.readLine(); // Read the header line
            if (header == null) {
                System.out.println("CSV file is empty.");
                return null;
            }
            String[] products = header.split(delimiter); // Split the header line to get product names
            // Read each line of the CSV file
            while ((line = br.readLine()) != null) {
                String[] data = line.split(delimiter);
                String branchID = data[0].trim();   // First column is branch ID
                Map<String, Integer> branchSales = new HashMap<>();
                for (int i = 1; i < data.length; i++) {
                    String productName = products[i].trim().replace(" ", "_");
                    int unitsSold = Integer.parseInt(data[i].trim());
                    branchSales.put(productName, unitsSold);
                } salesData.put(branchID, branchSales);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return salesData;
    }

    // Method to create a map of product objects with predefined profits
    private static Map<String, Product> createProducts() {
        Map<String, Product> products = new HashMap<>();
        products.put("Product_A", new Product("Product_A", 1.10));
        products.put("Product_B", new Product("Product_B", 1.50));
        products.put("Product_C", new Product("Product_C", 2.10));
        products.put("Product_D", new Product("Product_D", 1.60));
        products.put("Product_E", new Product("Product_E", 1.80));
        products.put("Product_F", new Product("Product_F", 3.90));
        return products;
    }

    // Method to calculate total units sold for each product
    private static Map<String, Integer> TotalUnitsSold_Calculator(Map<String, Map<String, Integer>> salesData) {
        Map<String, Integer> totalUnitsSold = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        // Process each branch's sales data concurrently
        for (Map<String, Integer> branch_Sales : salesData.values()) {
            executor.execute(() -> {
                for (Map.Entry<String, Integer> entry : branch_Sales.entrySet()) {
                    String product = entry.getKey().trim().replace(" ", "_"); // Sanitize product name
                    int unitsSold = entry.getValue();
                    totalUnitsSold.merge(product, unitsSold, Integer::sum); // To update total units sold for the product
                }
            });
        }
        // Shutdown the executor and wait for all tasks to finish
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);  // Wait for all tasks to complete, with no timeout limit
        } catch (InterruptedException e) {
            e.printStackTrace();  // To print the stack trace if the thread is interrupted
        }

        return totalUnitsSold;
    }

    // Method to calculate total daily profits
    private static double TotalDailyProfits_Calculator(Map<String, Map<String, Integer>> salesData, Map<String, Product> products, Map<String, Double> totalProductProfits, Map<String, Branch> branchProfits) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);   // Create an ExecutorService with a fixed thread pool
        AtomicReference<Double> totalDailyProfits = new AtomicReference<>(0.0);    // Use AtomicReference to handle concurrent updates to total daily profits
        // Process each branch's sales data concurrently
        for (Map.Entry<String, Map<String, Integer>> branch_Entry : salesData.entrySet()) {
            executor.execute(() -> {
                // Extract branch ID and create a new Branch object
                String branchID = branch_Entry.getKey();
                Branch branch = new Branch(branchID);
                // Iterate over each product and its sold units in the current branch's sales data
                for (Map.Entry<String, Integer> entry : branch_Entry.getValue().entrySet()) {
                    String product = entry.getKey().trim().replace(" ", "_"); // Sanitize product name
                    int unitsSold = entry.getValue();
                    Product productObj = products.get(product); // To get product object
                    System.out.println("Product Object: " + productObj);
                    if (productObj != null) {
                        double profit = unitsSold * productObj.getProfit(); // Calculate profit
                        System.out.println("Product: " + product + ", Units Sold: " + unitsSold + ", Profit: " + profit);
                        totalDailyProfits.updateAndGet(v -> v + profit);
                        branch.addProfit(profit); // To update branch profit
                        totalProductProfits.merge(product, profit, Double::sum);  // To update total profit for the product
                    } else {
                        System.out.println("Product not found: " + product);
                    }
                }
                branchProfits.put(branchID, branch);  // To store branch profit
            });
        }
        // Shutdown the executor and wait for all tasks to finish
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);   // Wait for all tasks to complete, with no timeout limit
        } catch (InterruptedException e) {
            e.printStackTrace();   // To print the stack trace if the thread is interrupted
        }

        return totalDailyProfits.get(); // Return the total daily profits
    }

    // Method to find the branch with the lowest profit
    private static Branch findBranchWithLowestProfit(Map<String, Branch> branchProfits) {
        return branchProfits.values().stream()     // To stream the values (Branch objects) from the branchProfits map
                .min((b1, b2) -> Double.compare(b1.getProfit(), b2.getProfit()))   // To find the minimum based on the profit of the branches
                .orElse(new Branch("No branches found"));  // If no branches are found, return a new Branch object with the message "No branches found"
    }
}
