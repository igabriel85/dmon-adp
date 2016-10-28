import os
import tempfile
import traceback
import weka.core.jvm as jvm
import weka.core.converters as converters
import dmonweka.helper as helper
from weka.classifiers import Classifier
from weka.experiments import SimpleCrossValidationExperiment, SimpleRandomSplitExperiment, Tester, ResultMatrix
import weka.plot.experiments as plot_exp


def main():
    """
    Run sample code.
    """

    print(helper.getDataDir())

    # cross-validation + classification
    helper.printTitle("Experiment: Cross-validation + classification")
    datasets = [helper.getDataDir() + os.sep + "iris.arff", helper.getDataDir() + os.sep + "anneal.arff"]
    classifiers = [Classifier("weka.classifiers.rules.ZeroR"), Classifier("weka.classifiers.trees.J48")]
    outfile = tempfile.gettempdir() + os.sep + "results-cv.arff"

    exp = SimpleCrossValidationExperiment(
        classification=True,
        runs=10,
        folds=10,
        datasets=datasets,
        classifiers=classifiers,
        result=outfile)
    exp.setup()
    exp.run()

    # evaluate
    loader = converters.loader_for_file(outfile)
    data = loader.load_file(outfile)
    matrix = ResultMatrix("weka.experiment.ResultMatrixPlainText")
    tester = Tester("weka.experiment.PairedCorrectedTTester")
    tester.resultmatrix = matrix
    comparison_col = data.attribute_by_name("Percent_correct").index
    tester.instances = data
    print(tester.header(comparison_col))
    print(tester.multi_resultset_full(0, comparison_col))

    # random split + regression
    helper.printTitle("Experiment: Random split + regression")
    datasets = [helper.getDataDir() + os.sep + "bolts.arff", helper.getDataDir() + os.sep + "bodyfat.arff"]
    classifiers = [
        Classifier("weka.classifiers.rules.ZeroR"),
        Classifier("weka.classifiers.functions.LinearRegression")
    ]
    outfile = tempfile.gettempdir() + os.sep + "results-rs.arff"
    exp = SimpleRandomSplitExperiment(
        classification=False,
        runs=10,
        percentage=66.6,
        preserve_order=False,
        datasets=datasets,
        classifiers=classifiers,
        result=outfile)
    exp.setup()
    exp.run()

    # evaluate
    loader = converters.loader_for_file(outfile)
    data = loader.load_file(outfile)
    matrix = ResultMatrix("weka.experiment.ResultMatrixPlainText")
    tester = Tester("weka.experiment.PairedCorrectedTTester")
    tester.resultmatrix = matrix
    comparison_col = data.attribute_by_name("Correlation_coefficient").index
    tester.instances = data
    print(tester.header(comparison_col))
    print(tester.multi_resultset_full(0, comparison_col))

    # plot
    plot_exp.plot_experiment(matrix, title="Random split", measure="Correlation coefficient", wait=True)

if __name__ == "__main__":
    try:
        jvm.start()
        main()
    except Exception, e:
        print(traceback.format_exc())
    finally:
        jvm.stop()