import dao.InventoryDao;
import dao.ShoppingCartDao;
import model.Inventory;
import model.ShoppingCart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Shopping cart main app
 * @author msameer
 */
public class ShoppingCartApp {

    /**
     * Shopping cart main
     * @param args
     */
   public static void main(String[] args) throws Exception {
       Configuration conf = HBaseConfiguration.create();
       initConf(conf);
       createTables(conf);
       InventoryDao inventoryDao = new InventoryDao(conf);
       ShoppingCartDao shoppingCartDao = new ShoppingCartDao(conf);

       if(args.length == 1 && args[0].equalsIgnoreCase("setup")) {
           System.out.printf("Starting setup");
           saveInventoryTableData(inventoryDao);
           printInventoryTableData(inventoryDao);
           saveShoppingCartData(shoppingCartDao);
           printShoppingCartDao(shoppingCartDao);
       } else if(args.length > 1 && args[0].equalsIgnoreCase("checkout")) {
           System.out.println("Starting user cart checkout");
           checkout(inventoryDao, shoppingCartDao, args[1]);
       } else if(args.length > 1 && args[0].equalsIgnoreCase("delete")) {
           System.out.println("Starting user cart deletion");
           deleteUserCart(args[1], shoppingCartDao);
       } else if(args.length == 1 && args[0].equalsIgnoreCase("delete")) {
           System.out.println("Deleting tables");
           deleteTable(inventoryDao, shoppingCartDao);
       } else if (args.length == 1 && args[0].equalsIgnoreCase("reset")) {
           System.out.println("Resetting tables");
           resetTables(conf);
       }
   }

    /**
     * Reset hbase tables
     * @param conf configuration
     */
    private static void resetTables(Configuration conf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        admin.disableTable(InventoryDao.TABLE_NAME);
        admin.deleteTable(InventoryDao.TABLE_NAME);

        admin.disableTable(ShoppingCartDao.TABLE_NAME);
        admin.deleteTable(ShoppingCartDao.TABLE_NAME);

        admin.close();
    }

    /**
     * Delete inventory and shopping cart table
     * @param inventoryDao inventory dao
     * @param shoppingCartDao shopping cart dao
     * @throws IOException
     */
    private static void deleteTable(InventoryDao inventoryDao, ShoppingCartDao shoppingCartDao) throws IOException {
        System.out.println("Deleting tables");
        inventoryDao.deleteInventories();
        shoppingCartDao.deleteShoppingCart();
        System.out.printf("Done");
    }

    /**
     * Check out shopping cart
     * @param inventoryDao inventory dao
     * @param shoppingCartDao shopping cart dao
     * @param cartId cart id
     */
    private static void checkout(InventoryDao inventoryDao, ShoppingCartDao shoppingCartDao, String cartId) throws IOException {
        System.out.println("Checking out cart for user:" + cartId);
        ShoppingCart shoppingCart = shoppingCartDao.getShoppingCart(cartId);
        checkout(inventoryDao, shoppingCart);
    }

    /**
     * Checkout shopping cart from inventory
     * @param inventoryDao inventory dao
     * @param shoppingCart shopping cart
     */
    private static void checkout(InventoryDao inventoryDao, ShoppingCart shoppingCart) throws IOException {
        inventoryDao.checkout("pens", shoppingCart.getShoppingCartId(), shoppingCart.getPens());
        Inventory pensInventory = inventoryDao.getInventory("pens");
        System.out.printf("Inventory for pens:" + pensInventory);

        inventoryDao.checkout("erasers", shoppingCart.getShoppingCartId(), shoppingCart.getErasers());
        Inventory erasersInventory = inventoryDao.getInventory("erasers");
        System.out.printf("Inventory for erasers:" + erasersInventory);

        inventoryDao.checkout("notebooks", shoppingCart.getShoppingCartId(), shoppingCart.getNotebooks());
        Inventory notebooksInventory = inventoryDao.getInventory("notebooks");
        System.out.printf("Inventory for notebooks:" + notebooksInventory);
    }

    /**
     * Delete user cart
     * @param cartId cart id
     */
    private static void deleteUserCart(String cartId, ShoppingCartDao shoppingCartDao) throws IOException {
        System.out.println("Deleting cart for: " + cartId);
        shoppingCartDao.deleteShoppingCart(cartId);
    }

    /**
     * Print shopping cart
     * @param shoppingCartDao shopping cart dao
     */
    private static void printShoppingCartDao(ShoppingCartDao shoppingCartDao) throws IOException {
        System.out.println("Print shopping cart data");
        List<ShoppingCart> shoppingCarts = shoppingCartDao.getShoppingCart();
        for(ShoppingCart shoppingCart : shoppingCarts) {
            System.out.println(shoppingCart);
        }
    }

    /**
     * Save shopping cart
     * @param shoppingCartDao shopping cart dao
     */
    private static void saveShoppingCartData(ShoppingCartDao shoppingCartDao) throws IOException {
        System.out.println("Save shopping cart data");
        shoppingCartDao.addToCart("Sameer", 1, 2, 3);
        shoppingCartDao.addToCart("Mark", 9, 0, 5);
    }

    /**
     * Print inventory table data
     * @param inventoryDao inventory dao
     */
    private static void printInventoryTableData(InventoryDao inventoryDao) throws IOException {
        System.out.println("Print inventory data");
        List<Inventory> inventoryList = inventoryDao.getInventories();
        for(Inventory inventory : inventoryList) {
            System.out.println(inventory);
        }
    }

    /**
     * Save inventory table data
     * @param inventoryDao inventory dao
     */
    private static void saveInventoryTableData(InventoryDao inventoryDao) throws IOException {
        System.out.println("Save inventory data");
        inventoryDao.addInventory("pens", 10);
        inventoryDao.addInventory("erasers", 20);
        inventoryDao.addInventory("notebooks", 30);
    }

    /**
     * Create tables
     * @param conf configuration
     * @throws IOException
     */
    private static void createTables(Configuration conf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        //Create  Inventory table
        createTable(admin, InventoryDao.TABLE_NAME, InventoryDao.STOCK_CF, 3);
        //Create ShoppingCart table
        createTable(admin, ShoppingCartDao.TABLE_NAME, ShoppingCartDao.ITEMS_CF, 3);

        admin.close();
    }

    /**
     *
     * @param admin hbase admin
     * @param tableName table name
     * @param columnFamily column family
     * @param versions versions
     * @throws IOException
     */
    private static void createTable(HBaseAdmin admin, byte[] tableName, byte[] columnFamily, int versions) throws IOException {
        if(admin.tableExists(tableName)) {
            System.out.println(Bytes.toString(tableName) + " table exists!");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
            columnDescriptor.setMaxVersions(versions);
            columnDescriptor.setCompressionType(Compression.Algorithm.LZ4);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
            System.out.println(Bytes.toString(tableName) + " table created!");
        }
    }

    /**
     * Initialize configuration
     * @param conf conf
     */
    private static void initConf(Configuration conf) {
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master", "localhost:60000");
    }
}