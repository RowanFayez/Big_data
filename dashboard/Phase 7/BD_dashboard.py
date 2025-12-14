import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import FactorAnalysis

cleaned_data = pd.read_csv("merged_data.csv")
monte_df = pd.read_csv("simulation_results.csv")
factor_scores = pd.read_csv("final_factor_scores_gold_layer_4_factors (1).csv")
factor_loadings = pd.read_csv("factor_loadings.csv")
factor_loadings.rename(columns={"Unnamed: 0": "Variable"}, inplace=True)

tab1, tab2, tab3 = st.tabs(
    ["📊 Dataset Statistics", "🎲 Monte Carlo Simulation", "📉 Factor Analysis"]
)

with tab1:
    st.header("Cleaned Dataset Statistics")

    st.subheader("Sample of Cleaned Data :")
    st.dataframe(cleaned_data.head(10))

    st.subheader("Shape of dataset:")
    st.write(cleaned_data.shape)

    st.subheader("Key Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", cleaned_data.shape[0])
    col2.metric("Total Accidents", int(cleaned_data["accident_count"].sum()))
    col3.metric("Average Speed (km/h)", round(cleaned_data["avg_speed_kmh"].mean(), 2))

    st.subheader("Summary Statistics:")
    st.dataframe(cleaned_data.describe())

    st.subheader("Missing Values:")
    null_df = cleaned_data.isnull().sum().reset_index()
    null_df.columns = ["Column Name", "Missing Values"]
    st.dataframe(null_df)

    column = st.selectbox(
        "Select a numeric column",
        cleaned_data.select_dtypes(include="number").columns,
        key="histogram",
    )
    fig, ax = plt.subplots()
    ax.hist(cleaned_data[column], bins=30)
    ax.set_title(f"Distribution of {column}")
    st.pyplot(fig)

    st.subheader("Distribution with Summary")
    num_col = st.selectbox(
        "Select numeric column",
        cleaned_data.select_dtypes(include="number").columns,
        key="summary_col",
    )
    col1, col2 = st.columns([2, 1])
    with col1:
        fig, ax = plt.subplots()
        ax.hist(cleaned_data[num_col], bins=30)
        ax.set_title(f"Distribution of {num_col}")
        st.pyplot(fig)

    with col2:
        st.write("Statistics")
        st.write(cleaned_data[num_col].describe())


with tab2:
    st.header("🎲 Monte Carlo Simulation Results")
    # 1. Simulation Overview
    st.subheader("📋 Simulation Overview")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Simulations", len(monte_df))
    with col2:
        st.metric(
            "Avg Congestion Prob", f"{monte_df['congestion_probability'].mean():.3f}"
        )
    with col3:
        st.metric("Avg Accident Prob", f"{monte_df['accident_probability'].mean():.3f}")

    # 2. Results Sample
    st.subheader("🔍 Simulation Results Sample")
    st.dataframe(monte_df.head(10))

    # 3. Probability Analysis
    st.subheader("📈 Probability Analysis")

    # Tabbed view for different analyses
    prob_tab1, prob_tab2 = st.tabs(["📊 Distributions", "📉 Comparisons"])

    with prob_tab1:
        # Distribution plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

        # Congestion Probability Distribution
        ax1.hist(
            monte_df["congestion_probability"],
            bins=20,
            color="orange",
            alpha=0.7,
            edgecolor="black",
        )
        ax1.set_title("Congestion Probability Distribution")
        ax1.set_xlabel("Probability")
        ax1.set_ylabel("Frequency")

        # Accident Probability Distribution
        ax2.hist(
            monte_df["accident_probability"],
            bins=20,
            color="red",
            alpha=0.7,
            edgecolor="black",
        )
        ax2.set_title("Accident Probability Distribution")
        ax2.set_xlabel("Probability")
        ax2.set_ylabel("Frequency")

        plt.tight_layout()
        st.pyplot(fig)

        # Summary statistics
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Congestion Probability Statistics:**")
            cong_stats = monte_df["congestion_probability"].describe()
            st.dataframe(pd.DataFrame(cong_stats).T)

        with col2:
            st.write("**Accident Probability Statistics:**")
            acc_stats = monte_df["accident_probability"].describe()
            st.dataframe(pd.DataFrame(acc_stats).T)

    with prob_tab2:
        # Scatter plot comparison
        fig, ax = plt.subplots(figsize=(10, 6))

        scatter = ax.scatter(
            monte_df["congestion_probability"],
            monte_df["accident_probability"],
            alpha=0.6,
            s=30,
        )

        ax.set_xlabel("Congestion Probability")
        ax.set_ylabel("Accident Probability")
        ax.set_title("Congestion vs Accident Probability")
        ax.grid(True, alpha=0.3)

        st.pyplot(fig)

        # Correlation analysis
        correlation = monte_df["congestion_probability"].corr(
            monte_df["accident_probability"]
        )
        st.metric("Correlation Coefficient", f"{correlation:.3f}")

        # Insights
        st.write("**Insights:**")
        if correlation > 0.3:
            st.info(
                "📈 Positive correlation: Higher congestion tends to be associated with higher accident probability"
            )
        elif correlation < -0.3:
            st.info(
                "📉 Negative correlation: Higher congestion tends to be associated with lower accident probability"
            )
        else:
            st.info(
                "📊 Weak correlation: No strong relationship between congestion and accident probabilities"
            )


with tab3:
    st.header("Factor Analysis Insights")

    factor_columns = factor_scores.columns[-4:]

    st.subheader("Factor Scores Distribution")
    selected_factor = st.selectbox(
        "Select Factor", factor_columns, key="factor_score_select"
    )

    fig, ax = plt.subplots()
    ax.hist(factor_scores[selected_factor], bins=30)
    ax.set_title(f"Distribution of {selected_factor}")
    st.pyplot(fig)

    st.subheader("Top & Bottom Records")
    n_records = st.slider(
        "Number of records to display",
        min_value=5,
        max_value=20,
        value=10,
        key="factor_n_records",
    )

    col1, col2 = st.columns(2)

    with col1:
        st.write("Highest Factor Scores")
        st.dataframe(
            factor_scores[[selected_factor]]
            .sort_values(by=selected_factor, ascending=False)
            .head(n_records)
        )

    with col2:
        st.write("Lowest Factor Scores")
        st.dataframe(
            factor_scores[[selected_factor]]
            .sort_values(by=selected_factor)
            .head(n_records)
        )

    st.subheader("Factor Interpretation (Loadings)")

    selected_loading_factor = st.selectbox(
        "Select Factor for Interpretation",
        factor_loadings.columns[1:],  # assuming first column is variable name
        key="factor_loading_select",
    )

    top_k = st.slider(
        "Top contributing variables",
        min_value=3,
        max_value=15,
        value=5,
        key="factor_top_k",
    )

    variable_col = factor_loadings.columns[0]

    loading_df = factor_loadings[[variable_col, selected_loading_factor]].copy()
    loading_df["Absolute Loading"] = loading_df[selected_loading_factor].abs()

    top_loadings = loading_df.sort_values(by="Absolute Loading", ascending=False).head(
        top_k
    )

    st.dataframe(top_loadings)

    # Bar plot for loadings
    fig, ax = plt.subplots()
    ax.barh(top_loadings["Variable"], top_loadings[selected_loading_factor])
    ax.set_title(f"Top Variables Contributing to {selected_loading_factor}")
    ax.invert_yaxis()
    st.pyplot(fig)
