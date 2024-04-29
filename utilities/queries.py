import pandas as pd
from utilities import database_utils as dbu


def execute_query(query):
    conn = dbu.create_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    dbu.close_connection(conn)
    return df


def spaning_questions_query():
    query = """
    SELECT
        a.QuestionID
    FROM
        Answer a
    WHERE
        a.SurveyID IN (2014, 2016, 2017, 2018, 2019)
    GROUP BY
        a.QuestionID
    HAVING
        COUNT(DISTINCT CASE WHEN a.SurveyID = 2014 THEN a.SurveyID END) > 0
        AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2016 THEN a.SurveyID END) > 0
        AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2017 THEN a.SurveyID END) > 0
        AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2018 THEN a.SurveyID END) > 0
        AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2019 THEN a.SurveyID END) > 0;
    """
    return execute_query(query)


def get_gender_distribution():
    query = """
    SELECT Survey.SurveyID,
           CASE
               WHEN LOWER(TRIM(AnswerText)) = 'male' THEN 'Male'
               WHEN LOWER(TRIM(AnswerText)) = 'female' THEN 'Female'
               WHEN TRIM(AnswerText) = '' OR AnswerText = '-1' THEN 'Unanswered'
               ELSE 'Other'
           END AS GenderGroup,
           COUNT(*) AS Count
    FROM Answer
    JOIN Question ON Answer.QuestionID = Question.QuestionID
    JOIN Survey ON Answer.SurveyID = Survey.SurveyID
    WHERE Question.QuestionID = 2
    GROUP BY Survey.SurveyID, GenderGroup;
    """
    return execute_query(query)


def usa_state_of_resedancy_query():
    query = """
    SELECT
        CASE 
            WHEN LOWER(TRIM(AnswerText)) = '-1' THEN 'No answer'
            WHEN LOWER(TRIM(AnswerText)) = 'washington' or LOWER(TRIM(AnswerText)) = 'dc' THEN 'washington DC'
            ELSE LOWER(TRIM(AnswerText))
        END AS CleanedAnswer,
        COUNT(*) AS Count
    FROM Answer
    JOIN (
        SELECT UserID
        FROM Answer
        JOIN Question ON Answer.QuestionID = Question.QuestionID
        WHERE Question.QuestionID = 3 AND LOWER(TRIM(AnswerText)) LIKE '%united states%'
    ) AS USRespondents ON Answer.UserID = USRespondents.UserID
    JOIN Question ON Answer.QuestionID = Question.QuestionID
    WHERE Question.QuestionID = 4
    GROUP BY CleanedAnswer
    ORDER BY Count DESC
    LIMIT 50;
    """
    return execute_query(query)


def country_of_resedancy_query():
    query = """
    SELECT
        CASE
            WHEN LOWER(TRIM(AnswerText)) = '-1' THEN 'No answer'
            WHEN LOWER(TRIM(AnswerText)) LIKE '%united states%' THEN 'USA'
            WHEN LOWER(TRIM(AnswerText)) LIKE '%america%' THEN 'USA'
            ELSE AnswerText
        END AS CleanedAnswer,
        COUNT(*) AS Count
    FROM Answer
    JOIN Question ON Answer.QuestionID = Question.QuestionID
    WHERE Question.QuestionID = 3
    GROUP BY CleanedAnswer
    ORDER BY Count DESC
    LIMIT 10;
    """
    return execute_query(query)


def final_dataframe_query():
    query = """
    SELECT
        a.UserID,
        a.QuestionID,
        q.QuestionText,
        a.SurveyID,
        LOWER(TRIM(a.AnswerText)) AS AnswerText
    FROM
        Answer a
    JOIN
        Question q ON a.QuestionID = q.QuestionID
    WHERE
        q.QuestionID IN (1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
            34, 48, 49, 50, 51, 52, 53, 54, 55, 56)
        AND a.SurveyID IN (2016, 2017, 2018, 2019)
    """
    return execute_query(query)


def spaning_questions_first_exclusion():
    query = """
    SELECT
        q.QuestionText
    FROM
        Question q
    WHERE
        q.QuestionID IN (
            SELECT
                a.QuestionID
            FROM
                Answer a
            WHERE
                a.SurveyID IN (2016, 2017, 2018, 2019)
            GROUP BY
                a.QuestionID
            HAVING
                COUNT(DISTINCT CASE WHEN a.SurveyID = 2016 THEN a.SurveyID END) > 0
                AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2017 THEN a.SurveyID END) > 0
                AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2018 THEN a.SurveyID END) > 0
                AND COUNT(DISTINCT CASE WHEN a.SurveyID = 2019 THEN a.SurveyID END) > 0
        );
    """
    return execute_query(query)


def survey_question_yes_no_query(question_id):
    query = f"""
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q{question_id},
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = {question_id}
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """
    return execute_query(query)

