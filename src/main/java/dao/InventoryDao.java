package dao;

import model.Inventory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * InventoryDao
 * @author msameer
 */
public class InventoryDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("Inventory");
    public static final byte[] STOCK_CF = Bytes.toBytes("stock");
    public static final byte[] QUANTITY_COL = Bytes.toBytes("quantity");

    private HTableInterface table = null;
    private Configuration conf = null;

    public InventoryDao(Configuration conf) throws IOException {
        this.conf = conf;
        this.table = new HTable(conf, TABLE_NAME);
    }

    /**
     * Add inventory
     * @param inventoryId inventory id
     * @param quantity quantity
     * @throws IOException
     */
    public void addInventory(String inventoryId, long quantity) throws IOException {
        Put put = new Put(Bytes.toBytes(inventoryId));
        put.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity));
        this.table.put(put);
    }

    /**
     * Get all inventories from
     * @return list of inventories
     */
    public List<Inventory> getInventories() throws IOException {
        List<Inventory> inventories = new ArrayList<>();
        Scan scan = new Scan();
        ResultScanner results = this.table.getScanner(scan);
        for(Result result : results) {
            Inventory inventory = new Inventory(result.getRow(), result.getValue(STOCK_CF, QUANTITY_COL));
            inventories.add(inventory);
        }

        return inventories;
    }

    /**
     * Get inventory
     * @param inventoryId inventory id
     * @return inventory
     */
    public Inventory getInventory(String inventoryId) throws IOException {
        Get get = new Get(Bytes.toBytes(inventoryId));
        Result result = this.table.get(get);
        Inventory inventory = new Inventory(result.getRow(), result.getValue(STOCK_CF, QUANTITY_COL));
        return inventory;
    }

    /**
     * Checkout inventory for cart id
     * @param inventoryId inventory id
     * @param cartId cart id
     * @param qty quantity
     */
    public void checkout(String inventoryId, String cartId, long qty) throws IOException {
        Inventory inventory = getInventory(inventoryId);
        if(inventory.getQuantity() < qty) {
            System.out.println("Inventory is short for:" + inventoryId);
        } else {
            long currQty = inventory.getQuantity();
            long newQty = currQty - qty;
            Put put = new Put(Bytes.toBytes(inventoryId));
            put.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(newQty));
            put.add(STOCK_CF, Bytes.toBytes(cartId), Bytes.toBytes(qty));
            this.table.checkAndPut(Bytes.toBytes(inventoryId), STOCK_CF, QUANTITY_COL, Bytes.toBytes(currQty), put);
        }
    }

    /**
     * Delete inventories
     * @throws IOException
     */
    public void  deleteInventories() throws IOException {
        Scan scan = new Scan();
        ResultScanner results = this.table.getScanner(scan);
        for(Result result : results) {
            Delete delete = new Delete(result.getRow());
            this.table.delete(delete);
        }
        this.table.close();
    }
}
