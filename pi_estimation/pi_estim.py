import random

import findspark
findspark.init()
import pyspark


def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1


def pi_estim(num_samples):
    sc = pyspark.SparkContext(appName="Pi")
    count = sc.parallelize(range(num_samples)).filter(inside).count()
    sc.stop()
    return 4 * count / num_samples


if __name__ == '__main__':
    num_samples = 10 ** 7
    pi = pi_estim(num_samples)
    print(f'Estimation of pi : {pi}')
