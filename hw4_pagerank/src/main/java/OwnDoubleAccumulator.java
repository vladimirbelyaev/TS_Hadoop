import org.apache.spark.AccumulatorParam;

class OwnDoubleAccumulator implements AccumulatorParam<Double> {
    @Override
    public Double zero(Double initialValue) {
        return 0.0;
    }

    @Override
    public Double addInPlace(Double r1, Double r2) {
        return r1+r2;
    }

    @Override
    public Double addAccumulator(Double r1, Double r2) {
        return r1+r2;
    }

}