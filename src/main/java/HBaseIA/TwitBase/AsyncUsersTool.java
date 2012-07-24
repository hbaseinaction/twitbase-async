package HBaseIA.TwitBase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class AsyncUsersTool {

  static final byte[] TABLE_NAME   = "users".getBytes();
  static final byte[] INFO_FAM     = "info".getBytes();
  static final byte[] PASSWORD_COL = "password".getBytes();
  static final byte[] EMAIL_COL    = "email".getBytes();

  public static final String usage =
    "usertool action ...\n" +
    "  help - print this message and exit.\n" +
    "  update - update passwords for all installed users.\n";

  static byte[] mkNewPassword(byte[] seed) {
    UUID u = UUID.randomUUID();
    return u.toString().replace("-", "").toLowerCase().getBytes();
  }

  static void latency() throws Exception {
    if (System.currentTimeMillis() % 3 == 0) {
      LOG.info("a thread is napping...");
      Thread.sleep(1000);
    }
  }

  static boolean entropy(Boolean val) {
    if (System.currentTimeMillis() % 5 == 0) {
      LOG.info("entropy strikes!");
      return false;
    }
    return (val == null) ? Boolean.TRUE : val;
  }

  static final class UpdateResult {
    public String userId;
    public boolean success;
  }

  @SuppressWarnings("serial")
  static final class UpdateFailedException extends Exception {
    public UpdateResult result;

    public UpdateFailedException(UpdateResult r) {
      this.result = r;
    }
  }

  @SuppressWarnings("serial")
  static final class SendMessageFailedException extends Exception {
    public SendMessageFailedException() {
      super("Failed to send message!");
    }
  }

  static final class InterpretResponse
      implements Callback<UpdateResult, Boolean> {

    private String userId;

    InterpretResponse(String userId) {
      this.userId = userId;
    }

    public UpdateResult call(Boolean response) throws Exception {
      latency();

      UpdateResult r = new UpdateResult();
      r.userId = this.userId;
      r.success = entropy(response);
      if (!r.success)
        throw new UpdateFailedException(r);

      latency();
      return r;
    }

    @Override
    public String toString() {
      return String.format("InterpretResponse<%s>", userId);
    }
  }

  static final class ResultToMessage
      implements Callback<String, UpdateResult> {

    public String call(UpdateResult r) throws Exception {
      latency();
      String fmt = "password change for user %s successful.";
      latency();
      return String.format(fmt, r.userId);
    }

    @Override
    public String toString() {
      return "ResultToMessage";
    }
  }

  static final class FailureToMessage
      implements Callback<String, UpdateFailedException> {

    public String call(UpdateFailedException e) throws Exception {
      latency();
      String fmt = "%s, your password is unchanged!";
      latency();
      return String.format(fmt, e.result.userId);
    }

    @Override
    public String toString() {
      return "FailureToMessage";
    }
  }

  static final class SendMessage
      implements Callback<Boolean, String> {

    public Boolean call(String s) throws Exception {
      latency();
      if (entropy(null))
        throw new SendMessageFailedException();
      LOG.info(s);
      latency();
      return Boolean.TRUE;
    }

    @Override
    public String toString() {
      return "SendMessage";
    }
  }

  static List<Deferred<Boolean>> doList(HBaseClient client)
      throws Throwable {
    final Scanner scanner = client.newScanner(TABLE_NAME);
    scanner.setFamily(INFO_FAM);
    scanner.setQualifier(PASSWORD_COL);

    ArrayList<ArrayList<KeyValue>> rows = null;
    ArrayList<Deferred<Boolean>> workers
      = new ArrayList<Deferred<Boolean>>();
    while ((rows = scanner.nextRows(1).joinUninterruptibly()) != null) {
      LOG.info("received a page of users.");
      for (ArrayList<KeyValue> row : rows) {
        KeyValue kv = row.get(0);
        byte[] expected = kv.value();
        String userId = new String(kv.key());
        PutRequest put = new PutRequest(
            TABLE_NAME, kv.key(), kv.family(),
            kv.qualifier(), mkNewPassword(expected));
        Deferred<Boolean> d = client.compareAndSet(put, expected)
          .addCallback(new InterpretResponse(userId))
          .addCallbacks(new ResultToMessage(), new FailureToMessage())
          .addCallback(new SendMessage());
        workers.add(d);
      }
    }
    return workers;
  }

  public static void main(String[] args) throws Throwable {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    final HBaseClient client = new HBaseClient("localhost");

    if ("update".equals(args[0])) {
      for(Deferred<Boolean> d: doList(client)) {
        try {
          d.join();
        } catch (SendMessageFailedException e) {
          LOG.info(e.getMessage());
        }
      }
    }

    client.shutdown().joinUninterruptibly();
  }

  static final Logger LOG = LoggerFactory.getLogger(AsyncUsersTool.class);
}
