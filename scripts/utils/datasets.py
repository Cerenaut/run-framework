from collections import defaultdict
import os
from random import Random
import shutil
import sys


def sample_dir(src_dirname, dst_dirname, sample_ratio=0.01, random_seed=0):
    label_to_filenames = defaultdict(list)
    for filename in sorted(os.listdir(src_dirname)):
        label_to_filenames[int(filename.split('_')[2])].append(filename)
    rnd = Random(int(random_seed))
    for filenames in label_to_filenames.values():
        rnd.shuffle(filenames)
        for filename in filenames[:int(float(sample_ratio) * len(filenames))]:
            shutil.copy(os.path.join(src_dirname, filename), os.path.join(dst_dirname, filename))

if __name__ == '__main__':
    sample_dir(*sys.argv[1:])