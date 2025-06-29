import numpy as np
import scipy.stats
from scipy.spatial.distance import cosine, pdist, squareform
from collections import defaultdict
import logging
import random

from db_manager import InstagramDataManager
from clustering import (
    get_following_network_data,
    perform_hdbscan_clustering
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_hypothesis_1(hdbscan_results, following_data):
    """
    H1: Creators in the same visual cluster follow each other more.
    """
    logger.info("\n--- [Original] Testing Hypothesis 1: Intra-cluster vs. Inter-cluster following ---")
    
    intra_cluster_edges = 0
    inter_cluster_edges = 0
    
    # Create a map of user_pk to cluster_id for quick lookups
    user_cluster_map = {pk: data['cluster'] for pk, data in hdbscan_results.items()}
    
    for follower_pk, followed_pks in following_data.items():
        if follower_pk not in user_cluster_map:
            continue
            
        follower_cluster = user_cluster_map[follower_pk]
        if follower_cluster == -1:  # Skip noise points as followers
            continue
            
        for followed_pk in followed_pks:
            if followed_pk not in user_cluster_map:
                continue
            
            followed_cluster = user_cluster_map[followed_pk]
            if followed_cluster == -1: # Skip noise points being followed
                continue

            if follower_cluster == followed_cluster:
                intra_cluster_edges += 1
            else:
                inter_cluster_edges += 1
                
    total_edges = intra_cluster_edges + inter_cluster_edges
    if total_edges == 0:
        logger.warning("No following edges found between clustered creators. Cannot test H1.")
        return

    intra_cluster_rate = (intra_cluster_edges / total_edges) * 100
    inter_cluster_rate = (inter_cluster_edges / total_edges) * 100

    logger.info(f"Total connections analyzed: {total_edges}")
    logger.info(f"Intra-cluster connections: {intra_cluster_edges} ({intra_cluster_rate:.2f}%)")
    logger.info(f"Inter-cluster connections: {inter_cluster_edges} ({inter_cluster_rate:.2f}%)")

    if intra_cluster_rate > inter_cluster_rate:
        logger.info("✅ [Original] H1 Confirmed: Creators are more likely to follow others within their own aesthetic cluster.")
    else:
        logger.info("❌ [Original] H1 Rejected: Following patterns do not show a strong preference for same-cluster creators.")


def test_hypothesis_1_permutation(hdbscan_results, following_data, n_permutations=1000):
    """
    H1 (Permutation Test): Tests if the observed intra-cluster connection rate is statistically significant.
    """
    logger.info("\n--- [Improved] Testing Hypothesis 1 (Permutation Test) ---")
    
    user_cluster_map = {pk: data['cluster'] for pk, data in hdbscan_results.items() if not data['is_noise']}
    if not user_cluster_map:
        logger.warning("No non-noise creators found for permutation test.")
        return
        
    nodes = list(user_cluster_map.keys())
    
    observed_intra_cluster_edges = 0
    total_edges = 0
    
    # Calculate the observed intra-cluster edge count among non-noise creators
    for follower_pk, followed_pks in following_data.items():
        if follower_pk not in user_cluster_map: continue
        for followed_pk in followed_pks:
            if followed_pk in user_cluster_map:
                total_edges += 1
                if user_cluster_map[follower_pk] == user_cluster_map[followed_pk]:
                    observed_intra_cluster_edges += 1
    
    if total_edges == 0:
        logger.warning("No following edges found between clustered creators. Cannot test H1.")
        return

    observed_rate = observed_intra_cluster_edges / total_edges
    logger.info(f"Observed intra-cluster connection rate: {observed_rate:.4f} ({observed_intra_cluster_edges}/{total_edges})")

    # Perform permutation test
    permuted_rates = []
    shuffled_clusters = list(user_cluster_map.values())

    for _ in range(n_permutations):
        random.shuffle(shuffled_clusters)
        shuffled_map = {node: cluster for node, cluster in zip(nodes, shuffled_clusters)}
        
        current_intra_cluster_edges = 0
        for follower_pk, followed_pks in following_data.items():
            if follower_pk not in shuffled_map: continue
            for followed_pk in followed_pks:
                if followed_pk in shuffled_map and shuffled_map[follower_pk] == shuffled_map[followed_pk]:
                    current_intra_cluster_edges += 1
        
        permuted_rates.append(current_intra_cluster_edges / total_edges)

    p_value = (np.sum(np.array(permuted_rates) >= observed_rate) + 1) / (n_permutations + 1)
    
    logger.info(f"Permutation test ({n_permutations} shuffles): Mean random rate={np.mean(permuted_rates):.4f}, p-value={p_value:.4f}")

    if p_value < 0.05:
        logger.info("✅ [Improved] H1 Confirmed: The observed rate of intra-cluster following is statistically significant.")
    else:
        logger.info("❌ [Improved] H1 Rejected: The observed rate is not statistically significant compared to random chance.")


def test_hypothesis_2_local_cohesion(hdbscan_results, creator_profiles, hdbscan_clusterer, k=5):
    """
    H2 (Local Cohesion): Creators more aligned with their local aesthetic (closer to k-nearest neighbors) have higher confidence.
    """
    logger.info(f"\n--- [Improved] Testing Hypothesis 2 (Local Cohesion with k={k}) ---")
    
    user_pks = list(creator_profiles.keys())
    confidence_scores = hdbscan_clusterer.probabilities_

    clusters = defaultdict(list)
    cluster_pk_map = defaultdict(list)
    for pk, data in hdbscan_results.items():
        if not data['is_noise']:
            clusters[data['cluster']].append(creator_profiles[pk])
            cluster_pk_map[data['cluster']].append(pk)
            
    local_cohesion_scores = []
    confidences_for_test = []
    
    user_to_confidence = {pk: confidence_scores[i] for i, pk in enumerate(user_pks)}

    for cluster_id, members in cluster_pk_map.items():
        if len(members) <= k: continue
        
        member_vectors = np.vstack([creator_profiles[pk] for pk in members])
        dist_matrix = squareform(pdist(member_vectors, 'cosine'))
        
        for i, pk in enumerate(members):
            creator_distances = dist_matrix[i]
            k_nearest_indices = np.argsort(creator_distances)[1:k+1]
            mean_dist = np.mean(creator_distances[k_nearest_indices])
            local_cohesion_scores.append(mean_dist)
            confidences_for_test.append(user_to_confidence[pk])

    if len(local_cohesion_scores) < 2:
        logger.warning("Not enough data to calculate correlation for H2. Need at least one cluster with > k members.")
        return

    res = scipy.stats.spearmanr(local_cohesion_scores, confidences_for_test)
    correlation, p_value = res.correlation, res.pvalue # type: ignore

    logger.info(f"Calculated local cohesion for {len(local_cohesion_scores)} non-noise creators.")
    logger.info(f"Spearman Correlation: {correlation:.4f}, P-value: {p_value:.4f}")

    if correlation < -0.2 and p_value < 0.05:
        logger.info("✅ [Improved] H2 Confirmed: Creators with higher local cohesion (lower distance to neighbors) have significantly higher confidence scores.")
    else:
        logger.info("❌ [Improved] H2 Rejected: No significant correlation found between local cohesion and confidence.")


def test_hypothesis_3_vector_bridge(hdbscan_results, creator_profiles, hdbscan_clusterer):
    """
    H3 (Vector Bridge): Creators aesthetically "between" clusters have lower confidence.
    """
    logger.info("\n--- [Improved] Testing Hypothesis 3 (Vector-Based Bridge Creators) ---")
    
    user_pks = list(creator_profiles.keys())
    confidence_scores = hdbscan_clusterer.probabilities_
    user_confidence_map = {pk: confidence_scores[i] for i, pk in enumerate(user_pks)}
    
    cluster_centroids = {}
    clusters = defaultdict(list)
    for pk, data in hdbscan_results.items():
        if not data['is_noise']:
            clusters[data['cluster']].append(creator_profiles[pk])
            
    if len(clusters) < 2:
        logger.warning("Need at least 2 clusters to test for bridge creators. Cannot test H3.")
        return
        
    for cluster_id, vectors in clusters.items():
        cluster_centroids[cluster_id] = np.mean(vectors, axis=0)

    bridge_scores = []
    confidences_for_test = []
    
    for pk, data in hdbscan_results.items():
        if data['is_noise']: continue
            
        own_cluster_id = data['cluster']
        profile_vector = creator_profiles[pk]
        d_own = cosine(profile_vector.flatten(), cluster_centroids[own_cluster_id].flatten())
        
        # Minimum distance to any *other* centroid
        other_distances = [cosine(profile_vector.flatten(), c.flatten()) for c_id, c in cluster_centroids.items() if c_id != own_cluster_id]
        if not other_distances:
            continue
        d_other = min(other_distances) # type: ignore
        
        if d_own > 1e-9: # Avoid division by zero
            bridge_scores.append(d_other / d_own)
            confidences_for_test.append(user_confidence_map[pk])

    if len(bridge_scores) < 2:
        logger.warning("Not enough data to calculate correlation for H3.")
        return

    res = scipy.stats.spearmanr(bridge_scores, confidences_for_test)
    correlation, p_value = res.correlation, res.pvalue # type: ignore

    logger.info(f"Calculated vector bridgeness for {len(bridge_scores)} non-noise creators.")
    logger.info(f"Spearman Correlation (bridgeness ratio vs. confidence): {correlation:.4f}, P-value: {p_value:.4f}")

    if correlation > 0.2 and p_value < 0.05:
        logger.info("✅ [Improved] H3 Confirmed: Creators more distinct from other clusters (less of a bridge) have higher confidence.")
    else:
        logger.info("❌ [Improved] H3 Rejected: No significant correlation found between vector-based bridgeness and confidence.")


def main():
    """
    Main function to load data and run all hypothesis tests.
    """
    logger.info("=== Starting Hypothesis Testing for Instagram Creator Clusters ===")
    
    db_manager = InstagramDataManager()
    creator_profiles, _ = db_manager.get_creator_profiles()
    
    if not creator_profiles:
        logger.critical("No creator profiles found. Aborting.")
        return
        
    hdbscan_results, hdbscan_clusterer = perform_hdbscan_clustering(creator_profiles)
    if not hdbscan_results:
        logger.critical("HDBSCAN clustering failed. Aborting.")
        return
        
    following_data = get_following_network_data(db_manager)
    
    # Run original descriptive tests
    test_hypothesis_1(hdbscan_results, following_data)
    # The original H2 and H3 tests from the prompt were conceptual, so we run the new ones.

    # Run new, more rigorous tests
    logger.info("\n\n=== Running More Rigorous Statistical Tests ===")
    test_hypothesis_1_permutation(hdbscan_results, following_data)
    test_hypothesis_2_local_cohesion(hdbscan_results, creator_profiles, hdbscan_clusterer)
    test_hypothesis_3_vector_bridge(hdbscan_results, creator_profiles, hdbscan_clusterer)


if __name__ == "__main__":
    main() 