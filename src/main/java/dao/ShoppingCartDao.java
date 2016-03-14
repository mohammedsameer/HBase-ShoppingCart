package dao;

import model.ShoppingCart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ShoppingCartDao
 * @author msameer
 */
public class ShoppingCartDao {

    public static final byte[] TABLE_NAME = Bytes.toBytes("ShoppingCart");
    public static final byte[] ITEMS_CF = Bytes.toBytes("items");
    public static final byte[] PENS_COL = Bytes.toBytes("pens");
    public static final byte[] ERASERS_COL = Bytes.toBytes("erasers");
    public static final byte[] NOTEBOOK_COL = Bytes.toBytes("notebooks");

    private HTableInterface table = null;
    private Configuration conf = null;

    public ShoppingCartDao(Configuration conf) throws IOException {
        this.conf = conf;
        this.table = new HTable(conf, TABLE_NAME);
    }

    /**
     * Add cart info
     * @param cartId cartId
     * @param pens pens
     * @param erasers erasers
     * @param notebooks notebooks
     * @throws IOException
     */
    public void addToCart(String cartId, long pens, long erasers, long notebooks) throws IOException {
        Put put = new Put(Bytes.toBytes(cartId));
        put.add(ITEMS_CF, PENS_COL, Bytes.toBytes(pens));
        put.add(ITEMS_CF, ERASERS_COL, Bytes.toBytes(erasers));
        put.add(ITEMS_CF, NOTEBOOK_COL, Bytes.toBytes(notebooks));
        this.table.put(put);
    }

    /**
     * Get shopping cart for cartId
     * @param cartId cartId
     * @return shopping cart
     * @throws IOException
     */
    public ShoppingCart getShoppingCart(String cartId) throws IOException {
        Get get = new Get(Bytes.toBytes(cartId));
        Result result = this.table.get(get);
        ShoppingCart shoppingCart = new ShoppingCart(result.getRow(), result.getValue(ITEMS_CF, PENS_COL),
                result.getValue(ITEMS_CF, ERASERS_COL), result.getValue(ITEMS_CF, NOTEBOOK_COL));
        return shoppingCart;
    }

    /**
     * Get shopping cart
     * @return shopping cart
     * @throws IOException
     */
    public List<ShoppingCart> getShoppingCart() throws IOException {
        List<ShoppingCart> shoppingCarts = new ArrayList<>();
        Scan scan = new Scan();
        ResultScanner results = this.table.getScanner(scan);
        for(Result result : results) {
            ShoppingCart shoppingCart = new ShoppingCart(result.getRow(), result.getValue(ITEMS_CF, PENS_COL),
                    result.getValue(ITEMS_CF, ERASERS_COL), result.getValue(ITEMS_CF, NOTEBOOK_COL));
            shoppingCarts.add(shoppingCart);
        }

        return shoppingCarts;
    }

    /**
     * Delete shopping cart
     * @throws IOException
     */
    public void deleteShoppingCart() throws IOException {
        Scan scan = new Scan();
        ResultScanner results = this.table.getScanner(scan);
        for(Result result : results) {
            Delete delete = new Delete(result.getRow());
            this.table.delete(delete);
        }
        this.table.close();
    }

    /**
     * Delete shopping cart
     * @throws IOException
     */
    public void deleteShoppingCart(String cartId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(cartId));
        this.table.delete(delete);
        this.table.close();
    }
}
