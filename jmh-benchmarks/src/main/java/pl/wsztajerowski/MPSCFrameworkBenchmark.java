package pl.wsztajerowski;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;

@State(Scope.Benchmark)
public class MPSCFrameworkBenchmark {
    MPSCFramework<Integer, Integer> framework;
    @Setup
    public void setup() throws IOException {
        framework = MPSCFramework.create(wrappers -> {
            for (Wrapper<Integer, Integer> wrapper : wrappers) {
                wrapper.response = wrapper.request + 1;
            }
        });
    }

    @TearDown
    public void tearDown() throws Exception {
        framework.close();
    }

    @Benchmark
    @GroupThreads(5)
    @Group("producers")
    public Integer produceElement() {
        return framework
            .produce(142);
    }
}
