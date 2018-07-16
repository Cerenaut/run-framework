"""SparseCapsExperiment class."""

import logging
import datetime

from agief_experiment import utils
from tf_experiment.experiment import Experiment

class SparseCapsExperiment(Experiment):
  """Experiment class for the SparseCaps project."""

  def run_sweeps(self, config, args, host_node, hparams_sweeps):
    """Run the sweeps"""
    if args.phase == 'train':
      prefixes = []
      print('........ Training\n')
      hparams_sweeps = self._parse_hparams_sweeps(hparams_sweeps)

      for i, hparams in enumerate(hparams_sweeps):
        run_prefix = datetime.datetime.now().strftime('%y%m%d-%H%M')
        prefixes.append(run_prefix)

        summary_dir = os.path.join(
            config['experiment-parameters']['summary_dir'],
            run_prefix)

        # Start experiment
        utils.remote_run(
            host_node,
            self._train_op(host_node.remote_variables_file,
                           config['experiment-parameters'],
                           config['train-parameters'],
                           summary_dir, hparams))

      with open('prefixes.txt', 'w') as prefix_file:
        prefix_file.write(','.join(prefixes))

    if args.phase == 'eval' or args.phase == 'classify':
      if args.prefixes is None:
        raise Exception('No prefixes provided.')

      prefixes = [x.strip() for x in args.prefixes.split(',')]

      for i, prefix in enumerate(prefixes):
        summary_dir = os.path.join(
            config['experiment-parameters']['summary_dir'],
            prefix)

        if args.phase == 'eval':
          # Export experiment for each prefix
          print('........ Evaluating: {0}\n'.format(prefix))
          for eval_sweep in config['eval-sweeps']:
            utils.remote_run(
                host_node,
                self._eval_op(host_node.remote_variables_file,
                              config['experiment-parameters'],
                              config['train-parameters'],
                              summary_dir, eval_sweep, hparams_sweeps[i]))

        if args.phase == 'classify':
          # Classification
          print('........ Classifying: {0}\n'.format(prefix))
          for classify_sweep in config['classify-sweeps']:
            for model in classify_sweep['model']:
              utils.remote_run(
                  host_node,
                  self._classify_op(host_node.remote_variables_file,
                                    summary_dir,
                                    classify_sweep['dataset'],
                                    model,
                                    config['train-parameters']['max_steps'],
                                    config['experiment-parameters']['model']))

  def _parse_hparams_sweeps(self, sweeps):
    hparams_sweeps = []
    for _, sweep in enumerate(sweeps):
      hparams_override = ''
      for param, value in sweep.items():
        hparams_override += '{0}={1},'.format(param, value)
      hparams_override = hparams_override[:-1]
      hparams_sweeps.append(hparams_override)
    return hparams_sweeps

  def _classify_op(self, variables_file, summary_dir, dataset, model, last_step, model_dir):
    """Start the classifier procedure via SSH."""
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python classifier.py --model={model} --dataset={dataset} \
        --data_dir=$TF_SUMMARY/{summary_dir}/classify/output \
        --summary_dir=$TF_SUMMARY/{summary_dir}/classify \
        --last_step={last_step}
    '''.format(
        variables_file=variables_file,
        summary_dir=summary_dir,
        dataset=dataset,
        model_dir=model_dir,
        model=model,
        last_step=last_step
    )

    logging.info(command)

    return command

  def _eval_op(self, variables_file, exp_params, train_params, summary_dir, eval_sweep, hparams):
    """Start the evaluation procedure via SSH."""
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python experiment.py --data_dir=$TF_DATA/{data_dir} --train=false \
        --checkpoint=$TF_SUMMARY/{summary_dir}/train/model.ckpt-{max_steps} \
        --summary_dir=$TF_SUMMARY/{summary_dir} --shift=0 --pad={pad} \
        --eval_set={eval_set} --eval_size={eval_size} --batch_size=100 \
        --eval_shard={eval_shard} --dataset={dataset} --num_gpus=1 \
        --hparams_override={hparams_override}
    '''.format(
        variables_file=variables_file,
        model_dir=exp_params['model'],
        max_steps=train_params['max_steps'],
        summary_dir=summary_dir,
        pad=eval_sweep['pad'],
        dataset=eval_sweep['dataset'],
        data_dir=train_params['dataset_path'],
        eval_set=eval_sweep['eval_set'],
        eval_shard=eval_sweep['eval_shard'],
        eval_size=eval_sweep['eval_size'],
        hparams_override=hparams
    )

    logging.info(command)

    return command

  def _train_op(self, vars_file, exp_params, train_params, summary_dir, hparams):
    """Start the training procedure via SSH."""
    command = '''
        export VARIABLES_FILE={variables_file}
        source {variables_file}
        source activate tensorflow
        cd $TF_HOME/{model_dir}
        python experiment.py --data_dir=$TF_DATA/{data_dir} \
        --summary_dir=$TF_SUMMARY/{summary_dir} --shift={shift} --pad={pad} \
        --batch_size={batch_size} --dataset={dataset} \
        --num_gpus={num_gpus} --max_steps={max_steps} \
        --hparams_override={hparams_override} \
        --summary_override=true
    '''.format(
        variables_file=vars_file,
        model_dir=exp_params['model'],
        num_gpus=exp_params['num_gpus'],
        max_steps=train_params['max_steps'],
        summary_dir=summary_dir,
        pad=train_params['pad'],
        shift=train_params['shift'],
        dataset=train_params['dataset'],
        batch_size=train_params['batch_size'],
        data_dir=train_params['dataset_path'],
        hparams_override=hparams
    )

    logging.info(command)

    return command
