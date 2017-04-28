from enum import Enum


class LaunchMode(Enum):
    per_experiment = 1
    per_session = 2

    @classmethod
    def from_args(cls, args):
        return cls.per_session if args.launch_per_session else cls.per_experiment
