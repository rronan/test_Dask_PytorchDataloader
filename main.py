import argparse
import logging
import traceback

from dask.distributed import Client, as_completed
from dask_jobqueue import SGECluster

logging.basicConfig(level=logging.INFO)


def init_cluster(args):
    env_extra = [
        "#$ -e {}".format(args.log_dir or "/dev/null"),
        "#$ -o {}".format(args.log_dir or "/dev/null"),
        "#$ -pe serial {}".format(args.ngpus if args.ngpus > 0 else args.ncpus),
        "export LANG=en_US.UTF-8",
        "export LC_ALL=en_US.UTF-8",
        "export MKL_NUM_THREADS=1",
        "export NUMEXPR_NUM_THREADS=1",
        "export OMP_NUM_THREADS=1",
        "export DISABLE_MP_CACHE=1",
    ]
    cluster = SGECluster(
        queue=args.queue,
        resource_spec="h_vmem={}G,mem_req={}G".format(args.h_vmem, args.mem_req),
        walltime="720:00:00",
        name="test_Dask_PytorchDataloader",
        cores=args.ncpus,
        memory="{}G".format(args.mem_req),
        processes=1,
        interface="ib0",
        local_directory=".",
        env_extra=env_extra,
        spill_dir=".",
        extra=["--no-nanny"],
    )
    cluster.scale(args.jobs)
    return cluster


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="gaia.q,zeus.q,titan.q,chronos.q")
    parser.add_argument("--mem_req", type=int, default=32)
    parser.add_argument("--h_vmem", type=int, default=200000)
    parser.add_argument("--ncpus", type=int, default=4)
    parser.add_argument("--ngpus", type=int, default=1)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    return args


def dummy_function(_):
    pass


def multiworkers_dataloader(_):
    pass


def main():
    args = parse_args()
    logging.info(args)
    cluster = init_cluster(args)
    client = Client(cluster)
    future_list = client.map(dummy_function, range(args.n_jobs))
    logging.info(cluster.job_script())
    for future in as_completed(future_list):
        exception = future.exception()
        traceback.print_exception(type(exception), exception, future.traceback())


if __name__ == "__main__":
    main()
