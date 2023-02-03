{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNfD50fVcp0V7KnACkhm1pn",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/sathiyajothi20/bigdata/blob/main/pyspark_mongodb.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 1,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "XNng-NZ8hQlN",
        "outputId": "9d04acfc-6f9d-4ea9-eac8-f46ad0617438"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/\n",
            "Collecting pyspark\n",
            "  Downloading pyspark-3.3.1.tar.gz (281.4 MB)\n",
            "\u001b[2K     \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m281.4/281.4 MB\u001b[0m \u001b[31m4.8 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n",
            "\u001b[?25h  Preparing metadata (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "Collecting py4j==0.10.9.5\n",
            "  Downloading py4j-0.10.9.5-py2.py3-none-any.whl (199 kB)\n",
            "\u001b[2K     \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m199.7/199.7 KB\u001b[0m \u001b[31m9.0 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n",
            "\u001b[?25hBuilding wheels for collected packages: pyspark\n",
            "  Building wheel for pyspark (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "  Created wheel for pyspark: filename=pyspark-3.3.1-py2.py3-none-any.whl size=281845512 sha256=a252daa2a5a9f442f7e7d7ce2190b9191faa2ca9319674f13734c518245ed51c\n",
            "  Stored in directory: /root/.cache/pip/wheels/43/dc/11/ec201cd671da62fa9c5cc77078235e40722170ceba231d7598\n",
            "Successfully built pyspark\n",
            "Installing collected packages: py4j, pyspark\n",
            "Successfully installed py4j-0.10.9.5 pyspark-3.3.1\n"
          ]
        }
      ],
      "source": [
        "!pip install pyspark"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql import SparkSession\n",
        "database = \"pyspark\" #your database name\n",
        "collection = \"policy\" #your collection name\n",
        "connectionString= ('mongodb+srv://sathiyajothi:kamala20@cluster0.wivk40m.mongodb.net/?retryWrites=true&w=majority')\n",
        "spark = SparkSession\\\n",
        ".builder\\\n",
        "    .config('spark.mongodb.input.uri',connectionString)\\\n",
        "    .config('spark.mongodb.output.uri', connectionString)\\\n",
        "    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\\\n",
        ".getOrCreate()\\\n",
        "# Reading from MongoDB\n",
        "df = spark.read\\\n",
        ".format(\"com.mongodb.spark.sql.DefaultSource\")\\\n",
        ".option(\"uri\", connectionString)\\\n",
        ".option(\"database\", database)\\\n",
        ".option(\"collection\", collection)\\\n",
        ".load()"
      ],
      "metadata": {
        "id": "icCzlQiujY6o"
      },
      "execution_count": 3,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "to15UmsFrOJD",
        "outputId": "a44c3826-e146-4ad8-f926-a0e0aeb20d40"
      },
      "execution_count": 7,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "|                 _id|country|              detail|  end_date|          gov_policy|policy_id|start_date|       type|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "|{63d39c4220480a52...|  Korea|      Level 1 (Blue)|2020-01-19|Infectious Diseas...|        1|2020-01-03|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|    Level 2 (Yellow)|2020-01-27|Infectious Diseas...|        2|2020-01-20|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|    Level 3 (Orange)|2020-02-22|Infectious Diseas...|        3|2020-01-28|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|       Level 4 (Red)|      null|Infectious Diseas...|        4|2020-02-23|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|          from China|      null|Special Immigrati...|        5|2020-02-04|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|      from Hong Kong|      null|Special Immigrati...|        6|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Macau|      null|Special Immigrati...|        7|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Japan|      null|Special Immigrati...|        8|2020-03-09|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Italy|      null|Special Immigrati...|        9|2020-03-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from Iran|      null|Special Immigrati...|       10|2020-03-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|         from France|      null|Special Immigrati...|       11|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|        from Germany|      null|Special Immigrati...|       12|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Spain|      null|Special Immigrati...|       13|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from U.K.|      null|Special Immigrati...|       14|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|    from Netherlands|      null|Special Immigrati...|       15|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|         from Europe|      null|Special Immigrati...|       16|2020-03-16|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Special Immigrati...|       17|2020-03-19|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Mandatory 14-day ...|       18|2020-04-01|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from U.S.|      null|Mandatory Self-Qu...|       19|2020-04-13|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|             1st EUA|      null|Emergency Use Aut...|       20|2020-02-04|     Health|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import count\n",
        "print(\"the number of rows\",df.count())\n",
        "print(\"describe\",df.describe())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "KZNHERhoujBc",
        "outputId": "7151ec10-9ae2-466d-f9f3-9269df414589"
      },
      "execution_count": 10,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "the number of rows 61\n",
            "describe DataFrame[summary: string, country: string, detail: string, end_date: string, gov_policy: string, policy_id: string, start_date: string, type: string]\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df_=df.drop_duplicates().show()\n",
        "import pandas as pd\n",
        "\n",
        "df_=pd.DataFrame(df_)\n",
        "print(df.count())"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "_PbtTdjUvbbc",
        "outputId": "72581c50-e4e8-4f51-85d4-817094476f4a"
      },
      "execution_count": 20,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+--------------+\n",
            "|                 _id|country|              detail|  end_date|          gov_policy|policy_id|start_date|          type|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+--------------+\n",
            "|{63d39c4220480a52...|  Korea|          from Macau|      null|Special Immigrati...|        7|2020-02-12|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|             2nd EUA|      null|Emergency Use Aut...|       21|2020-02-12|        Health|\n",
            "|{63d39c4220480a52...|  Korea| by Local Government|      null|Drive-Through Scr...|       25|2020-02-26|        Health|\n",
            "|{63d39c4220480a52...|  Korea|        Kindergarten|2020-04-06|School Opening Delay|       35|2020-03-02|     Education|\n",
            "|{63d39c4220480a52...|  Korea|           Weak(1st)|      null|Social Distancing...|       32|2020-05-06|        Social|\n",
            "|{63d39c4220480a52...|  Korea|5-day Rotation Sy...|      null|   Mask Distribution|       28|2020-03-09|        Health|\n",
            "|{63d39c4220480a52...|  Korea|              Strong|2020-03-21|Social Distancing...|       29|2020-02-29|        Social|\n",
            "|{63d39c4220480a52...|  Korea|On-site inspectio...|2020-06-11|    Logistics center|       57|2020-05-29|Transformation|\n",
            "|{63d39c4220480a52...|  Korea|         from France|      null|Special Immigrati...|       11|2020-03-15|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|         from Europe|      null|Special Immigrati...|       16|2020-03-16|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|    Level 2 (Yellow)|2020-01-27|Infectious Diseas...|        2|2020-01-20|         Alert|\n",
            "|{63d39c4220480a52...|  Korea|High School (1st ...|2020-06-03|School Opening wi...|       41|2020-04-16|     Education|\n",
            "|{63d39c4220480a52...|  Korea|Middle School (3r...|2020-05-27|School Opening wi...|       42|2020-04-09|     Education|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Mandatory 14-day ...|       18|2020-04-01|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|Gathering limited...|      null|local government ...|       55|2020-05-16|Administrative|\n",
            "|{63d39c4220480a52...|  Korea|             5th EUA|      null|Emergency Use Aut...|       24|2020-03-13|        Health|\n",
            "|{63d39c4220480a52...|  Korea|           from U.K.|      null|Special Immigrati...|       14|2020-03-15|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Special Immigrati...|       17|2020-03-19|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Japan|      null|Special Immigrati...|        8|2020-03-09|   Immigration|\n",
            "|{63d39c4220480a52...|  Korea|    from Netherlands|      null|Special Immigrati...|       15|2020-03-15|   Immigration|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+--------------+\n",
            "only showing top 20 rows\n",
            "\n",
            "61\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df.limit(10).show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "GbE802wY17cx",
        "outputId": "151aff3e-c49f-46f3-98d6-3e0f078d55d3"
      },
      "execution_count": 22,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+-------+----------------+----------+--------------------+---------+----------+-----------+\n",
            "|                 _id|country|          detail|  end_date|          gov_policy|policy_id|start_date|       type|\n",
            "+--------------------+-------+----------------+----------+--------------------+---------+----------+-----------+\n",
            "|{63d39c4220480a52...|  Korea|  Level 1 (Blue)|2020-01-19|Infectious Diseas...|        1|2020-01-03|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|Level 2 (Yellow)|2020-01-27|Infectious Diseas...|        2|2020-01-20|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|Level 3 (Orange)|2020-02-22|Infectious Diseas...|        3|2020-01-28|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|   Level 4 (Red)|      null|Infectious Diseas...|        4|2020-02-23|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|      from China|      null|Special Immigrati...|        5|2020-02-04|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|  from Hong Kong|      null|Special Immigrati...|        6|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|      from Macau|      null|Special Immigrati...|        7|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|      from Japan|      null|Special Immigrati...|        8|2020-03-09|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|      from Italy|      null|Special Immigrati...|        9|2020-03-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|       from Iran|      null|Special Immigrati...|       10|2020-03-12|Immigration|\n",
            "+--------------------+-------+----------------+----------+--------------------+---------+----------+-----------+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df=df.withColumnRenamed(\"gov_policy\", \"govt_policy\")\n",
        "df.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "gU87XxM02YXG",
        "outputId": "26572c54-9ff9-4744-af79-77d0db2e2096"
      },
      "execution_count": 24,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "|                 _id|country|              detail|  end_date|         govt_policy|policy_id|start_date|       type|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "|{63d39c4220480a52...|  Korea|      Level 1 (Blue)|2020-01-19|Infectious Diseas...|        1|2020-01-03|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|    Level 2 (Yellow)|2020-01-27|Infectious Diseas...|        2|2020-01-20|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|    Level 3 (Orange)|2020-02-22|Infectious Diseas...|        3|2020-01-28|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|       Level 4 (Red)|      null|Infectious Diseas...|        4|2020-02-23|      Alert|\n",
            "|{63d39c4220480a52...|  Korea|          from China|      null|Special Immigrati...|        5|2020-02-04|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|      from Hong Kong|      null|Special Immigrati...|        6|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Macau|      null|Special Immigrati...|        7|2020-02-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Japan|      null|Special Immigrati...|        8|2020-03-09|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Italy|      null|Special Immigrati...|        9|2020-03-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from Iran|      null|Special Immigrati...|       10|2020-03-12|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|         from France|      null|Special Immigrati...|       11|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|        from Germany|      null|Special Immigrati...|       12|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|          from Spain|      null|Special Immigrati...|       13|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from U.K.|      null|Special Immigrati...|       14|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|    from Netherlands|      null|Special Immigrati...|       15|2020-03-15|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|         from Europe|      null|Special Immigrati...|       16|2020-03-16|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Special Immigrati...|       17|2020-03-19|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|from all the coun...|      null|Mandatory 14-day ...|       18|2020-04-01|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|           from U.S.|      null|Mandatory Self-Qu...|       19|2020-04-13|Immigration|\n",
            "|{63d39c4220480a52...|  Korea|             1st EUA|      null|Emergency Use Aut...|       20|2020-02-04|     Health|\n",
            "+--------------------+-------+--------------------+----------+--------------------+---------+----------+-----------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyspark.sql.functions import col\n",
        "df=df.select(col(\"_id\"),col(\"govt_policy\")).show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "mgkE18yz3D4_",
        "outputId": "8792b92f-7887-4fae-835a-75ce262b69ad"
      },
      "execution_count": 27,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+--------------------+\n",
            "|                 _id|         govt_policy|\n",
            "+--------------------+--------------------+\n",
            "|{63d39c4220480a52...|Infectious Diseas...|\n",
            "|{63d39c4220480a52...|Infectious Diseas...|\n",
            "|{63d39c4220480a52...|Infectious Diseas...|\n",
            "|{63d39c4220480a52...|Infectious Diseas...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Special Immigrati...|\n",
            "|{63d39c4220480a52...|Mandatory 14-day ...|\n",
            "|{63d39c4220480a52...|Mandatory Self-Qu...|\n",
            "|{63d39c4220480a52...|Emergency Use Aut...|\n",
            "+--------------------+--------------------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [],
      "metadata": {
        "id": "prAU3Yvf3M3c"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "collection=\"seoulFloating\"\n",
        "seoul_df= spark.read\\\n",
        ".format(\"com.mongodb.spark.sql.DefaultSource\")\\\n",
        ".option(\"uri\", connectionString)\\\n",
        ".option(\"database\", database)\\\n",
        ".option(\"collection\", collection)\\\n",
        ".load()"
      ],
      "metadata": {
        "id": "hE__D2l29iL3"
      },
      "execution_count": 28,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "seoul_df.show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "iG3YSv679wlY",
        "outputId": "c116ad84-9b70-45b3-a464-6bbdc3dab0af"
      },
      "execution_count": 29,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------------+----------+-------------+----------+------+----+--------+------+\n",
            "|                 _id|birth_year|         city|      date|fp_num|hour|province|   sex|\n",
            "+--------------------+----------+-------------+----------+------+----+--------+------+\n",
            "|{63d39ca320480a52...|        20|    Dobong-gu|2020-01-01| 19140|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|    Dobong-gu|2020-01-01| 19950|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|Dongdaemun-gu|2020-01-01| 25450|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|Dongdaemun-gu|2020-01-01| 27050|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|   Dongjag-gu|2020-01-01| 28880|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|   Dongjag-gu|2020-01-01| 30350|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20| Eunpyeong-gu|2020-01-01| 27750|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20| Eunpyeong-gu|2020-01-01| 27910|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|   Gangbuk-gu|2020-01-01| 19490|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|   Gangbuk-gu|2020-01-01| 21940|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|  Gangdong-gu|2020-01-01| 27800|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|  Gangdong-gu|2020-01-01| 30390|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|   Gangnam-gu|2020-01-01| 45680|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|   Gangnam-gu|2020-01-01| 43680|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|   Gangseo-gu|2020-01-01| 37780|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|   Gangseo-gu|2020-01-01| 38300|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20| Geumcheon-gu|2020-01-01| 14220|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20| Geumcheon-gu|2020-01-01| 16100|   0|   Seoul|  male|\n",
            "|{63d39ca320480a52...|        20|      Guro-gu|2020-01-01| 26040|   0|   Seoul|female|\n",
            "|{63d39ca320480a52...|        20|      Guro-gu|2020-01-01| 27080|   0|   Seoul|  male|\n",
            "+--------------------+----------+-------------+----------+------+----+--------+------+\n",
            "only showing top 20 rows\n",
            "\n"
          ]
        }
      ]
    }
  ]
}