package inc.future.redispq;

import java.io.Serializable;

public interface Prioritizable extends Serializable {
    Long getPriority();
    String getPayload();
    String getUUID();
}
