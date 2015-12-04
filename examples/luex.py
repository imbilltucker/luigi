
import luigi


class ComplexTask(luigi.Task):
    """ This call the simple task before performing it's task"""

    x = luigi.IntParameter()
    y = luigi.IntParameter(default=32)

    def requires(self):
        return SimpleTask(self.x, self.y)

    def run(self):
        print self.x + self.y


class SimpleTask(luigi.Task):

    x = luigi.IntParameter()
    y = luigi.IntParameter(default=45)

    def run(self):
        print self.x * self.y

    @luigi.Task.event_handler(luigi.Event.SUCCESS)
    def celebrate_success(self):
        """
        Report success event
        Uses "-+--+--+--+--+--+--+--+--+--+-" as indicator
        """

        print(26 * '-+-')
        print("Yay!, {c} succeeded. :)".format(c=self.__class__.__name__))
        print(26 * '-+-')

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def mourn_failure(self, exception):
        """
        Report failure event
        Uses "-!--!--!--!--!--!--!--!--!--!-"  as indicator
        """

        print(26 * '-!-')
        print("Boo!, {c} failed.  :(".format(c=self.__class__.__name__))
        print(".. with this exception: '{e}'".format(e=str(exception)))
        print(26 * '-!-')


def main():
    """ This task can be run by: python <filename.py> <classname> <args>
        For example:
            python luex.py ComplexTask --x=10
    """
    luigi.run()




if __name__ == "__main__":
    main()

