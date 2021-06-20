package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class FileQueueService implements QueueService {
  private final String queueDir;

  // The character used to separate fields of the record in the message file.
  // Message ID should never contain this character.
  private final String fieldDelimiter;

  // Visibility Timeout (in seconds).
  // How long time the service wait for a delivered message to be acknowledged/deleted
  // before the service make the message visible for delivery again.
  private final int visibilityTimeout;

  // An optional object to tell the queue service current time.
  // Set it when we want an alternative time to the system time, e.g. when test.
  private LongSupplier timeSupplier;

  public FileQueueService() {
    Properties confInfo = new Properties();

    try (InputStream inStream =
        getClass().getClassLoader().getResourceAsStream("config.properties")) {
      confInfo.load(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    queueDir = confInfo.getProperty("queueDirectory", "nzhou-qs");
    fieldDelimiter = confInfo.getProperty("fieldDelimiter", ":");
    visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
  }

  public void setTimeSupplier(LongSupplier timeSupplier) {
    this.timeSupplier = timeSupplier;
  }

  private void lock(File lock) throws InterruptedException {
    while (!lock.mkdir()) {
      Thread.sleep(50);
    }
  }

  private void unlock(File lock) {
    lock.delete();
  }

  @Override
  public void push(String queueUrl, String messageBody) {
    String queueName = fromUrl(queueUrl);
    File messages = getMessagesFile(queueName);
    File lock = getLockFile(queueName);
    try {
      lock(lock);
    } catch (InterruptedException e) {
      e.printStackTrace();
      unlock(lock);
      return;
    }

    if (Files.notExists(messages.toPath())) {
      try {
        // Create the empty file with default permissions.
        Files.createFile(messages.toPath());
      } catch (IOException e) {
        // Some sort of failure, such as permissions.
        System.err.format("createFile error: %s%n", e);
        unlock(lock);
        return;
      }
    }

    try (PrintWriter pw = new PrintWriter(new FileWriter(messages, true))) { // append
      pw.println(createRecord(0, messageBody));
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      unlock(lock);
    }
  }

  @Override
  public Message pull(String queueUrl) {
    Message msg = null;
    String queueName = fromUrl(queueUrl);
    File messageFile = getMessagesFile(queueName);
    File lock = getLockFile(queueName);

    try {
      lock(lock);
    } catch (InterruptedException e2) {
      unlock(lock);
      return null;
    }

    // create a temporary file
    Path queuePath = Paths.get(queueDir);
    Path tempFile;
    try {
      tempFile = Files.createTempFile(queuePath, null, ".msg");
    } catch (IOException e1) {
      unlock(lock);
      return null;
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(messageFile));
        PrintWriter pwTemp = new PrintWriter(new FileWriter(tempFile.toFile(), true))) {
      String msgLine = null;

      while ((msgLine = reader.readLine()) != null) {
        if (msg == null) {
          msg = getVisibleMessage(msgLine);

          if (msg == null) {
            pwTemp.println(msgLine);
          } else {
            pwTemp.println(getDeliveredRecord(msgLine, msg.getReceiptId()));
          }
        } else {
          pwTemp.println(msgLine);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try { // if msg has been set then update the queue file
        if (msg != null) {
          Files.move(tempFile, messageFile.toPath(), REPLACE_EXISTING);
        } else {
          Files.delete(tempFile);
        }
      } catch (IOException x) {
      }

      unlock(lock);
    }

    return msg;
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    String queueName = fromUrl(queueUrl);
    File messageFile = getMessagesFile(queueName);
    File lock = getLockFile(queueName);
    Path tempFile;

    try {
      lock(lock);

      // create a temporary file
      Path queuePath = Paths.get(queueDir);
      tempFile = Files.createTempFile(queuePath, null, ".msg");
    } catch (InterruptedException | IOException e) {
      unlock(lock);
      return;
    }

    boolean processed = false;

    try (BufferedReader reader = new BufferedReader(new FileReader(messageFile));
        PrintWriter writer = new PrintWriter(new FileWriter(tempFile.toFile(), true))) {
      String msgLine = null;
      while ((msgLine = reader.readLine()) != null) {
        if (isToDelete(msgLine, receiptId)) {
          processed = true;
        } else {
          writer.println(msgLine);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try { // if processed then update the queue file
        if (processed) {
          Files.move(tempFile, messageFile.toPath(), REPLACE_EXISTING);
        } else {
          Files.delete(tempFile);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      unlock(lock);
    }
  }

  /**
   * Deletes the messages in a queue specified by parameter queueUrl.
   *
   * @param queueUrl
   */
  protected void purgeQueue(String queueUrl) {
    String queueName = fromUrl(queueUrl);
    File messageFile = getMessagesFile(queueName);
    File lock = getLockFile(queueName);

    try {
      lock(lock);
    } catch (InterruptedException e) {
      e.printStackTrace();
      unlock(lock);
      return;
    }

    if (Files.exists(messageFile.toPath())) {
      try {
        Files.delete(messageFile.toPath());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      // Create the empty file with default permissions.
      Files.createFile(messageFile.toPath());
    } catch (IOException e) {
    }

    unlock(lock);
  }

  /**
   * Get queue name from the queue URL specified by parameter queueUrl. A queue URL is like:
   * https://sqs.us-east-1.amazonaws.com/<account-id>/<queue-name>
   *
   * @param queueUrl
   * @return
   */
  private String fromUrl(String queueUrl) {
    String[] strArr = queueUrl.split("/");
    int len = strArr.length;

    if (!strArr[len - 1].equals("")) {
      return strArr[len - 1];
    } else {
      if (len > 1) {
        return strArr[len - 2];
      } else {
        return strArr[len - 1];
      }
    }
  }

  /** Get File for the queue */
  private File getMessagesFile(String queueName) {
    Path path = Paths.get(queueDir, queueName, "messages");
    return path.toFile();
  }

  /**
   * Get the lock file for the queue, and also make sure the directory for the queue exists.
   *
   * @param queueName
   * @return
   */
  private File getLockFile(String queueName) {
    Path queueFolder = Paths.get(queueDir, queueName);

    if (Files.notExists(queueFolder)) {
      // Create the folder with default permissions, etc.
      try {
        Files.createDirectories(queueFolder);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    Path path = Paths.get(queueDir, queueName, ".lock");
    return path.toFile();
  }

  //
  // Message record helper functions.
  // Record format:
  //		<prior attempts>delimiter<visible from time>delimiter<receipt id>delimiter<message body>
  //            0													1													2										3
  //

  private String createRecord(long visibleFrom, String message) {
    return "0" + fieldDelimiter + visibleFrom + fieldDelimiter + fieldDelimiter + message;
  }

  /**
   * If the record represent a visible message then construct a message from the record.
   *
   * @return A message; null if the record does not represent a visible message.
   */
  private Message getVisibleMessage(String record) {
    if (record == null || record == "") {
      return null;
    }

    String[] fields = record.split(fieldDelimiter, 4);

    if (Long.parseLong(fields[1]) < now()) {
      Message msg = new Message(fields[3], UUID.randomUUID().toString());

      return msg;
    } else {
      return null;
    }
  }

  /**
   * Update the message record after delivery. This will increase attempts by 1, set visibleFrom to
   * visibility timeout from now, update receipt id specified parameter receiptId.
   *
   * @param
   * @return
   */
  private String getDeliveredRecord(String record, String receiptId) {
    String[] fields = record.split(fieldDelimiter, 4);
    if (fields.length < 4) {
      return record;
    }

    int attempts = Integer.parseInt(fields[0]) + 1;
    long visibleFrom = now() + TimeUnit.SECONDS.toMillis(visibilityTimeout);

    return attempts
        + fieldDelimiter
        + visibleFrom
        + fieldDelimiter
        + receiptId
        + fieldDelimiter
        + fields[3];
  }

  /**
   * Determine whether the record should be deleted.
   *
   * @param record
   * @param receiptId
   * @return true if the record's receiptId matches the receiptId specified by parameter receiptId
   *     and the record is invisible; false otherwise.
   */
  private boolean isToDelete(String record, String receiptId) {
    String[] fields = record.split(fieldDelimiter, 4);
    if (fields.length < 4) {
      return true;
    }

    if (Long.parseLong(fields[1]) >= now() && fields[2].equals(receiptId)) {
      return true;
    }

    return false;
  }

  long now() {
    return this.timeSupplier == null ? System.currentTimeMillis() : timeSupplier.getAsLong();
  }
}
