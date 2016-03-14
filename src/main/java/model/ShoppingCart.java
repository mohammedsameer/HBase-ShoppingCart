package model;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * ShoppingCart object
 */
public class ShoppingCart {

    private String shoppingCartId;
    private long pens;
    private long erasers;
    private long notebooks;

    public ShoppingCart(String shoppingCartId, long pens, long erasers, long notebooks) {
        this.shoppingCartId = shoppingCartId;
        this.pens = pens;
        this.erasers = erasers;
        this.notebooks = notebooks;
    }

    public ShoppingCart(byte[] shoppingCartId, byte[] pens, byte[] erasers, byte[] notebooks) {
        this.shoppingCartId = Bytes.toString(shoppingCartId);
        this.pens = Bytes.toLong(pens);
        this.erasers = Bytes.toLong(erasers);
        this.notebooks = Bytes.toLong(notebooks);
    }


    public String getShoppingCartId() {
        return shoppingCartId;
    }

    public long getPens() {
        return pens;
    }

    public long getErasers() {
        return erasers;
    }

    public long getNotebooks() {
        return notebooks;
    }


    @Override
    public String toString() {
        return "[ShoppingCart { shoppingCartId: " + shoppingCartId + ", pens: " + pens + ", erasers: " + erasers + ", notebooks: "+ notebooks + "}]";
    }
}
