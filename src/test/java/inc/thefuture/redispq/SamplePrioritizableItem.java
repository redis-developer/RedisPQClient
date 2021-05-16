package inc.thefuture.redispq;

import inc.future.redispq.Prioritizable;
import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@AllArgsConstructor
@Setter
public class SamplePrioritizableItem implements Prioritizable {

    private Long priority;

    @Override
    public Long getPriority() {
        return this.priority;
    }

    @Override
    public String getPayload() {
        return "{}";
    }

    @Override
    public String getUUID() {
        return UUID.randomUUID().toString();
    }
}
