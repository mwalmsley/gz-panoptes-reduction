from gzreduction.schemas.schema import Schema, Question, Answer
# https://github.com/mwalmsley/gz-panoptes-reduction/blob/master/gzreduction/schemas/cd_schema.py


_questions = [
    Question(
        name='expert',
        raw_name='T0',
        answers=[
            Answer(
                name='grade_a',
                raw_name='grade a (confidently a lens)'),
            Answer(
                name='grade_b',
                raw_name='grade b (probably a lens, additional info needed)'),
            Answer(
                name='grade_c',
                raw_name='grade c (lens-like features could have other explanation)'),
            Answer(
                name='not_lens_but_interesting',
                raw_name='not a lens, but otherwise interesting'),
            Answer(
                name='not_lens_not_interesting',
                raw_name='not a lens, not interesting')
        ]
    )
]

judges_schema = Schema(_questions)