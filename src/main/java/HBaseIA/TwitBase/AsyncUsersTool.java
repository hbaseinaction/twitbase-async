package HBaseIA.TwitBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class AsyncUsersTool {

  static final String TABLE_NAME = "users";
  static final String INFO_FAM   = "info";

  static final byte[] USER_COL   = "user".getBytes();
  static final byte[] NAME_COL   = "name".getBytes();
  static final byte[] EMAIL_COL  = "email".getBytes();

  public static final String usage =
    "usertool action ...\n" +
    "  help - print this message and exit.\n" +
    "  list - list all installed users.\n";

  static String userStringFromKeyValues(List<KeyValue> row) {
    StringBuilder sb = new StringBuilder("<User: ");
    String userName = null, name = null, email = null;
    for (KeyValue kv : row) {
      if (Arrays.equals(kv.qualifier(), USER_COL))
        userName = new String(kv.value());
      else if (Arrays.equals(kv.qualifier(), NAME_COL))
        name = new String(kv.value());
      else if (Arrays.equals(kv.qualifier(), EMAIL_COL))
        email = new String(kv.value());
    }
    sb.append(userName).append(", ")
      .append(name).append(", ")
      .append(email).append(">");
    return sb.toString();
  }

  static void doList(HBaseClient client) throws Throwable {
    final Scanner scanner = client.newScanner(TABLE_NAME);
    scanner.setFamily(INFO_FAM);
    scanner.setMaxNumKeyValues(-1);

    List<Deferred<List<String>>> deferreds
      = new ArrayList<Deferred<List<String>>>();
    ArrayList<ArrayList<KeyValue>> rows = null;
    while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
      for(ArrayList<KeyValue> row : rows) {
        System.out.println(userStringFromKeyValues(row));
      }
    }

    for(Deferred<List<String>> d: deferreds) {
      for(String user : d.join())
        System.out.println(user);
    }
  }

  public static void main(String[] args) throws Throwable {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    final HBaseClient client = new HBaseClient("localhost");

    if ("list".equals(args[0])) {
      doList(client);
    }

    client.shutdown().joinUninterruptibly();
  }
}
