
import pandas as pd

from gzreduction.vote_sources.panoptes_exports import responses_to_votes

# move the question rows to columns
def test_subject_question_table_to_votes(cleaned_responses, schema, votes):
    result = responses_to_votes.get_votes(
        pd.DataFrame(cleaned_responses),
        question_col='task',
        answer_col='value',
        schema=schema)

    # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
    c1 = result[result['classification_id'] == 'c1']
    assert c1['question-0_features-or-disk'].sum() == 1
    assert c1['question-1_round'].sum() == 0
    assert c1['question-2_no'].sum() == 1
    assert c1['question-9_no'].sum() == 0
    assert c1['question-9_yes'].sum() == 1

    # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
    c2 = result[result['classification_id'] == 'c2']
    assert c2['question-0_features-or-disk'].sum() == 0
    assert c2['question-1_round'].sum() == 1
    assert c1['question-2_no'].sum() == 1
    assert c2['question-9_no'].sum() == 1
    assert c2['question-9_yes'].sum() == 0

    for col in votes:
        assert result[col].equals(votes[col])
