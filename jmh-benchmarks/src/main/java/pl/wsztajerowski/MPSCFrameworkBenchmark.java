package pl.wsztajerowski;

import org.openjdk.jmh.annotations.*;

import java.util.function.Consumer;

@State(Scope.Benchmark)
public class MPSCFrameworkBenchmark {
    MPSCFramework<Integer, Integer> framework;


    public static class WrapperConsumer implements Consumer<Wrapper<Integer, Integer>[]> {
        @Override
        public void accept(Wrapper<Integer, Integer>[] wrappers) {
            for (Wrapper<Integer, Integer> wrapper : wrappers) {
                wrapper.response = wrapper.request + 1;
            }
        }
    }

    @Setup
    public void setup() {
        framework = MPSCFramework.create(new WrapperConsumer());
    }

    @TearDown
    public void tearDown() throws Exception {
        framework.close();
    }

    @Benchmark
    @Group("producers")
    public Integer produceElement() {
        return framework
                .produce(142);
    }
}
