import sqlite3
import pandas as pd


conn = sqlite3.connect('Datasets/mental_health.sqlite')
cursor = conn.cursor()


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

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


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

    cursor.execute(query)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['CleanedAnswer', 'Count'])
    return df


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

    cursor.execute(query)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['CleanedAnswer', 'Count'])
    return df


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

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


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

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
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

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def mental_history_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q6,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 6
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def treated_by_proffesional_query():
    query = """
    SELECT
        SurveyID,
        SUM(CASE WHEN AnswerText = '1' THEN 1 ELSE 0 END) AS Yes_Count,
        COUNT(*) AS Total_Answers_Q7
    FROM
        Answer
    WHERE
        SurveyID != 2014 AND QuestionID = 7
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def openess_during_jobhunt_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q12,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 12
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def openess_with_coworkers_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q18,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 18
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def openess_with_superior_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q19,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 19
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def past_ilness_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q32,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 32
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def current_ilness_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q33,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 33
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def ever_ilness_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q34,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 34
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def openess_with_client_query():
    query = """
    SELECT
        SurveyID,
        COUNT(*) AS Total_Answers_Q53,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'yes' THEN 1 ELSE 0 END) AS Yes_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) = 'no' THEN 1 ELSE 0 END) AS No_Count,
        SUM(CASE WHEN LOWER(TRIM(AnswerText)) NOT IN ('yes', 'no') THEN 1 ELSE 0 END) AS Other_Count
    FROM
        Answer
    WHERE
        QuestionID = 53
        AND SurveyID != 2014
    GROUP BY
        SurveyID;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df
