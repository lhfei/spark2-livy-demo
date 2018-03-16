package cn.lhfei.spark2.livy;

import java.util.ArrayList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class PiJob implements Job<Double>, Function<Integer, Integer>, Function2<Integer, Integer, Integer> {
	private static final long serialVersionUID = -7787627046604633208L;

	private int samples = 0;

	public PiJob(int samples) {
		this.samples = samples;
	}

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + v2;
	}

	@Override
	public Integer call(Integer v1) throws Exception {
		double x = Math.random();
		double y = Math.random();

		return ((x * x + y * y) < 1) ? 1 : 0;
	}

	@Override
	public Double call(JobContext context) throws Exception {
		List<Integer> sampleList = new ArrayList<>();
		for (int i = 0; i < samples; i++) {
			sampleList.add(i + 1);
		}
		return 3.0d * context.sc().parallelize(sampleList).map(this).reduce(this) / samples;
	}

}
