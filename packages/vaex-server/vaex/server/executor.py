
class Executor:
    def __init__(self, client):
        self.client = client
        self.tasks = []

    def schedule(self, task):
        self.tasks.append(task)

    def _rmi(self, df, methodname, args, kwargs):
        # TODO: turn evaluate into a task
        return self.client._rmi(df, methodname, args, kwargs)

    def execute(self):
        tasks = list(self.tasks)
        # import pdb; pdb.set_trace()
        try:
            for task in tasks:
                dfs = set(task.df for task in tasks)
                for df in dfs:
                    tasks_df = [task for task in tasks if task.df is df]
                    for task in tasks_df:
                        task.signal_progress.emit(0)
                    # tasks_df_specs = [task.spec for task in tasks_df]
                    # tasks_df_serialized = [task.serialize() for task in tasks_df]
                    results = self.client.execute(df, tasks_df)
                    for task, result in zip(tasks_df, results):
                        task._result = result
                        task.fulfill(result)
        finally:
            self.tasks = []
