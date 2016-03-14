package model;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Inventory object
 */
public class Inventory {

    private String inventoryId;
    private Long quantity;

    public Inventory(byte[] inventoryId, byte[] quantity) {
        this.inventoryId = Bytes.toString(inventoryId);
        this.quantity = Bytes.toLong(quantity);
    }

    public Inventory(String inventoryId, Long quantity) {
        this.inventoryId = inventoryId;
        this.quantity = quantity;
    }

    public String getInventoryId() {
        return inventoryId;
    }

    public Long getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "[Inventory { inventoryId: " + inventoryId + ", quantity: " + quantity + " }]";
    }
}
