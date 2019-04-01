
class Schema(object):

    def __init__(self):
        self.questions = []

    def add_question(self, question):
        self.questions.append(question)

    def get_question_names(self):
        return [question.name for question in self.questions]

    def get_raw_question_names(self):
        return [question.raw_name for question in self.questions]

    def get_question_from_name(self, question_name):
        return list(filter(lambda x: x.name == question_name, self.questions))[0]

    def get_question_from_raw_name(self, question_raw_name):  # needs test
        return list(filter(lambda x: x.raw_name == question_raw_name, self.questions))[0]

    def get_count_columns(self):
        return [question.get_count_column(answer) for question in self.questions for answer in question.answers]

    def get_raw_count_columns(self):
        return [question.get_raw_count_column(answer) for question in self.questions for answer in question.answers]

    def get_fraction_columns(self):
        return [question.get_fraction_column(answer) for question in self.questions for answer in question.answers]

    def rename_df_using_schema(self, df):
        old_columns = self.get_raw_count_columns()
        new_columns = self.get_count_columns()
        return df.copy().rename(columns=dict(zip(old_columns, new_columns)))


class Question(object):

    def __init__(self, name, raw_name):
        self.name = name
        self.raw_name = raw_name
        self.total_votes = name + '_total-votes'
        self.prediction = name + '_prediction'
        self.prediction_conf = name + '_prediction-conf'
        self.prediction_encoded = name + '_prediction-encoded'
        self.truth_encoded = name + '_truth-encoded'
        self.answers = None

    def __repr__(self):
        return 'Question {}'.format(self.name)


    def set_answers(self, answers):
        [answer.set_question(self) for answer in answers]
        self.answers = answers

    def get_answer_from_name(self, answer_name):
        return list(filter(lambda x: x.name == answer_name, self.answers))[0]

    def get_answer_from_raw_name(self, answer_raw_name):
        return list(filter(lambda x: x.raw_name == answer_raw_name, self.answers))[0]

    def get_answer_names(self):
        return [answer.name for answer in self.answers]

    def get_raw_answer_names(self):
        return [answer.raw_name for answer in self.answers]

    def get_count_column(self, answer):
        return self.name + '_' + answer.name

    def get_answer_from_count_column(self, count_col):
        # could be stored for speed
        count_to_answer_mapping = dict([(self.get_count_column(answer), answer) for answer in self.answers])
        return count_to_answer_mapping[count_col]

    def get_raw_count_column(self, answer):
        return self.raw_name + '_' + answer.raw_name

    def get_count_columns(self):
        return [self.get_count_column(answer) for answer in self.answers]

    def get_all_raw_count_columns(self):
        return [self.get_raw_count_column(answer) for answer in self.answers]

    def get_fraction_column(self, answer):
        return self.name + '_' + answer.name + '_fraction'

    def get_fractions_for_question(self):
        return [self.get_fraction_column(answer) for answer in self.answers]

    def get_fraction_min_col(self, answer):
        return self.name + '_' + answer.name + '_min'

    def get_fraction_max_col(self, answer):
        return self.name + '_' + answer.name + '_max'


class Answer(object):

    def __init__(self, name, raw_name):
        self.name = name
        self.raw_name = raw_name
        self.question = None

    def set_question(self, question):
        self.question = question
    
    def __repr__(self):
        return 'Answer {} to {}'.format(self.name, self.question)
