package pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 *
 */
public class AppPubSubConsumer
{
    public static void main( String[] args )
    {
        String projectId = "kinetic-dryad-395306";
        String subscriptionId = "gcp-pubsub-demo-sub1";
        subscribeEg(projectId, subscriptionId);

    }

    private static void subscribeEg(String projectId, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s: \n", subscriptionName);
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch(TimeoutException e) {
            subscriber.stopAsync();
        }
    }


}
