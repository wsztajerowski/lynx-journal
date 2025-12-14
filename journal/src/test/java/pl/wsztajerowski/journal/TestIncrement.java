package pl.wsztajerowski.journal;

import com.vmlens.api.AllInterleavings;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrement {

    private int j = 0;

    @Test
    public void testIncrement() throws InterruptedException {
        try(AllInterleavings allInterleavings = new AllInterleavings("tutorial")) {
            while (allInterleavings.hasNext()) {
                j = 0;
                Thread first = new Thread() {
                    @Override
                    public void run() {
                        j++;
                    }
                };
                first.start();
                j++;
                first.join();
                assertThat(j)
                    .isEqualTo(2);
            }
        }
    }

}