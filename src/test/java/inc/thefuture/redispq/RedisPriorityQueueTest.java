package inc.thefuture.redispq;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import inc.future.redispq.Prioritizable;
import inc.future.redispq.RedisPriorityQueue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static inc.thefuture.redispq.TestConstants.APP_NAME;
import static inc.thefuture.redispq.TestConstants.REDIS_HOSTNAME;
import static inc.thefuture.redispq.TestConstants.REGION;

public class RedisPriorityQueueTest {

    @Test
    public void integrationTest() throws IOException {
        RedisPriorityQueue redisPriorityQueue = new RedisPriorityQueue(
                APP_NAME, new DefaultAWSCredentialsProviderChain(), REGION, REDIS_HOSTNAME, null
        );
        redisPriorityQueue.add(new SamplePrioritizableItem(100L));
        redisPriorityQueue.add(new SamplePrioritizableItem(100L));
        redisPriorityQueue.add(new SamplePrioritizableItem(1000L));
        redisPriorityQueue.add(new SamplePrioritizableItem(10L));
        redisPriorityQueue.add(new SamplePrioritizableItem(10000L));
        redisPriorityQueue.add(new SamplePrioritizableItem(10000L));

        Prioritizable prioritizable = redisPriorityQueue.peek();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(10000L));

        prioritizable = redisPriorityQueue.peek();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(10000L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(10000L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(10000L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(1000L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(100L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(100L));

        prioritizable = redisPriorityQueue.poll();
        System.out.println(prioritizable.getPriority());
        Assert.assertEquals(prioritizable.getPriority(), Long.valueOf(10L));
    }
}
