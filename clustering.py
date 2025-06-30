import numpy as np
from collections import defaultdict
from sklearn.cluster import KMeans
import umap
import matplotlib.pyplot as plt
from db_manager import InstagramDataManager
import logging
import hdbscan
import warnings
from sklearn.preprocessing import MinMaxScaler
from matplotlib.patches import Ellipse
import matplotlib.transforms as transforms
import networkx as nx
import json
import sqlite3
import matplotlib.lines as mlines

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def confidence_ellipse(x, y, ax, n_std=2.0, **kwargs):
    """
    Create a plot of the covariance confidence ellipse of *x* and *y*.
    
    Parameters
    ----------
    x, y : array-like, shape (n, )
        Input data.
    ax : matplotlib.axes.Axes
        The axes object to draw the ellipse into.
    n_std : float
        The number of standard deviations to determine the ellipse's radiuses.
    **kwargs
        Forwarded to `~matplotlib.patches.Ellipse`
    
    Returns
    -------
    matplotlib.patches.Ellipse
    """
    if x.size != y.size:
        raise ValueError("x and y must be the same size")

    cov = np.cov(x, y)
    pearson = cov[0, 1]/np.sqrt(cov[0, 0] * cov[1, 1])
    
    # Using a special case to obtain the eigenvalues of this
    # two-dimensional dataset.
    ell_radius_x = np.sqrt(1 + pearson)
    ell_radius_y = np.sqrt(1 - pearson)
    ellipse = Ellipse((0, 0), width=ell_radius_x * 2, height=ell_radius_y * 2, **kwargs)

    # Calculating the standard deviation of x from the square root of
    # the variance and multiplying with the given number of standard deviations.
    scale_x = np.sqrt(cov[0, 0]) * n_std
    mean_x = np.mean(x)

    # calculating the standard deviation of y ...
    scale_y = np.sqrt(cov[1, 1]) * n_std
    mean_y = np.mean(y)

    transf = transforms.Affine2D() \
        .rotate_deg(45) \
        .scale(scale_x, scale_y) \
        .translate(mean_x, mean_y)

    ellipse.set_transform(transf + ax.transData)
    return ax.add_patch(ellipse)

def perform_clustering(creator_profiles, n_clusters=5):
    """Perform K-means clustering on creator profiles"""
    print(f"\n=== Starting Clustering Analysis (K={n_clusters}) ===")
    
    if not creator_profiles:
        print("No creator profiles to cluster")
        return None, None
    
    # Convert profiles to matrix
    user_pks = list(creator_profiles.keys())
    profile_matrix = np.array([creator_profiles[pk].flatten() for pk in user_pks])  # Flatten to ensure 2D
    
    print(f"Profile matrix shape: {profile_matrix.shape}")
    
    # Perform K-means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
    cluster_labels = kmeans.fit_predict(profile_matrix)
    
    # Create results dictionary
    clustering_results = {}
    for i, user_pk in enumerate(user_pks):
        clustering_results[user_pk] = {
            'cluster': int(cluster_labels[i]),
            'profile_vector': profile_matrix[i]
        }
    
    # Analyze clusters
    cluster_sizes = {}
    for label in cluster_labels:
        cluster_sizes[label] = cluster_sizes.get(label, 0) + 1
    
    print(f"Cluster sizes: {cluster_sizes}")
    
    return clustering_results, kmeans

def perform_hdbscan_clustering(creator_profiles, min_cluster_size=8, min_samples=4, cluster_selection_epsilon=0.0):
    """Perform HDBSCAN clustering on creator profiles with configurable parameters"""
    print(f"\n=== Starting HDBSCAN Clustering Analysis ===")
    print(f"Parameters: min_cluster_size={min_cluster_size}, min_samples={min_samples}, cluster_selection_epsilon={cluster_selection_epsilon}")
    
    if not creator_profiles:
        print("No creator profiles to cluster")
        return None, None
    
    # Convert profiles to matrix
    user_pks = list(creator_profiles.keys())
    profile_matrix = np.array([creator_profiles[pk].flatten() for pk in user_pks])  # Flatten to ensure 2D
    
    print(f"Profile matrix shape: {profile_matrix.shape}")
    
    # Perform HDBSCAN clustering
    # Using UMAP for dimensionality reduction first (HDBSCAN works better with lower dimensions)
    umap_reducer = umap.UMAP(n_components=50, random_state=42, n_neighbors=15, min_dist=0.1)
    umap_embedding = umap_reducer.fit_transform(profile_matrix)
    
    # Perform HDBSCAN on UMAP embedding with configurable parameters
    hdbscan_clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, 
        min_samples=min_samples, 
        metric='euclidean',
        cluster_selection_epsilon=cluster_selection_epsilon
    )
    cluster_labels = hdbscan_clusterer.fit_predict(umap_embedding)
    
    # Create results dictionary
    clustering_results = {}
    for i, user_pk in enumerate(user_pks):
        clustering_results[user_pk] = {
            'cluster': int(cluster_labels[i]),
            'profile_vector': profile_matrix[i],
            'is_noise': cluster_labels[i] == -1
        }
    
    # Analyze clusters
    cluster_sizes = {}
    noise_count = 0
    for label in cluster_labels:
        if label == -1:
            noise_count += 1
        else:
            cluster_sizes[label] = cluster_sizes.get(label, 0) + 1
    
    print(f"HDBSCAN found {len(cluster_sizes)} clusters")
    print(f"Cluster sizes: {cluster_sizes}")
    print(f"Noise points: {noise_count}")
    
    return clustering_results, hdbscan_clusterer

def generate_umap_coordinates(creator_profiles):
    """Generate UMAP coordinates for creator profiles"""
    print("\n=== Generating UMAP Coordinates ===")
    
    if not creator_profiles:
        print("No creator profiles to process")
        return None, None
    
    # Prepare data
    user_pks = list(creator_profiles.keys())
    profile_matrix = np.array([creator_profiles[pk].flatten() for pk in user_pks])
    
    # Generate UMAP coordinates
    umap_model = umap.UMAP(n_components=2, random_state=42)
    umap_result = umap_model.fit_transform(profile_matrix)
    
    # Ensure umap_result is a dense numpy array
    umap_result = np.asarray(umap_result)
    
    # Normalize coordinates to [0, 1] range for better storage and visualization
    scaler = MinMaxScaler()
    umap_normalized = scaler.fit_transform(umap_result)
    
    # Create coordinates dictionary with normalized values
    creator_coordinates = {}
    for i, user_pk in enumerate(user_pks):
        creator_coordinates[user_pk] = (float(umap_normalized[i, 0]), float(umap_normalized[i, 1]))
    
    print(f"Generated normalized UMAP coordinates for {len(creator_coordinates)} creators")
    print(f"Coordinate range: X=[{umap_normalized[:, 0].min():.3f}, {umap_normalized[:, 0].max():.3f}], Y=[{umap_normalized[:, 1].min():.3f}, {umap_normalized[:, 1].max():.3f}]")
    return creator_coordinates, umap_normalized

def visualize_clusters(creator_coordinates, clustering_results, n_clusters=5):
    """Visualize clusters using stored UMAP coordinates with ellipses"""
    print("\n=== Creating K-means Visualizations ===")
    
    if not creator_coordinates or not clustering_results:
        print("No data to visualize")
        return
    
    # Prepare data for visualization
    user_pks = list(creator_coordinates.keys())
    coordinates = np.array([creator_coordinates[pk] for pk in user_pks])
    cluster_labels = [clustering_results[pk]['cluster'] for pk in user_pks]
    
    # Create UMAP visualization
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Define colors for clusters
    cmap = plt.get_cmap('viridis')
    colors = cmap(np.linspace(0, 1, n_clusters))
    
    # Plot individual points and add ellipses
    for cluster_id in range(n_clusters):
        cluster_indices = [i for i, label in enumerate(cluster_labels) if label == cluster_id]
        if cluster_indices:
            cluster_coords = coordinates[cluster_indices]
            
            # Plot individual points
            ax.scatter(cluster_coords[:, 0], cluster_coords[:, 1], 
                      c=[colors[cluster_id]], alpha=0.7, s=50, 
                      label=f'Cluster {cluster_id}' if cluster_id == 0 else "")
            
            # Add confidence ellipse (2 standard deviations)
            if len(cluster_coords) > 2:  # Need at least 3 points for covariance
                confidence_ellipse(cluster_coords[:, 0], cluster_coords[:, 1], ax, 
                                 n_std=2.0, alpha=0.2, color=colors[cluster_id], 
                                 linewidth=2, linestyle='--')
    
    plt.colorbar(plt.cm.ScalarMappable(cmap='viridis'), ax=ax, label='Cluster')
    plt.title('Creator Profiles - K-means Clustering (UMAP) with Ellipses')
    plt.xlabel('UMAP Component 1')
    plt.ylabel('UMAP Component 2')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig('creator_clusters_umap.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("Visualization saved as:")
    print("- creator_clusters_umap.png")

def visualize_hdbscan_clusters(creator_coordinates, hdbscan_results):
    """Visualize HDBSCAN clusters using stored UMAP coordinates with ellipses"""
    print("\n=== Creating HDBSCAN Visualizations ===")
    
    if not creator_coordinates or not hdbscan_results:
        print("No HDBSCAN data to visualize")
        return
    
    # Prepare data for visualization
    user_pks = list(creator_coordinates.keys())
    coordinates = np.array([creator_coordinates[pk] for pk in user_pks])
    cluster_labels = [hdbscan_results[pk]['cluster'] for pk in user_pks]
    is_noise = [hdbscan_results[pk]['is_noise'] for pk in user_pks]
    
    # Create UMAP visualization
    fig, ax = plt.subplots(figsize=(16, 14))
    
    # Get non-noise cluster IDs
    cluster_ids = sorted(set([label for label in cluster_labels if label != -1]))
    cmap = plt.get_cmap('viridis')
    colors = cmap(np.linspace(0, 1, len(cluster_ids)))
    
    # Plot regular points with cluster colors and add ellipses
    for i, cluster_id in enumerate(cluster_ids):
        cluster_indices = [j for j, label in enumerate(cluster_labels) if label == cluster_id and not is_noise[j]]
        if cluster_indices:
            cluster_coords = coordinates[cluster_indices]
            
            # Plot individual points
            ax.scatter(cluster_coords[:, 0], cluster_coords[:, 1], 
                      c=[colors[i]], alpha=0.7, s=50, 
                      label=f'Cluster {cluster_id}' if i == 0 else "")
            
            # Add confidence ellipse (2 standard deviations)
            if len(cluster_coords) > 2:  # Need at least 3 points for covariance
                confidence_ellipse(cluster_coords[:, 0], cluster_coords[:, 1], ax, 
                                 n_std=2.0, alpha=0.2, color=colors[i], 
                                 linewidth=2, linestyle='--')
    
    # Plot noise points in gray
    noise_indices = [i for i in range(len(user_pks)) if is_noise[i]]
    if noise_indices:
        noise_coordinates = coordinates[noise_indices]
        ax.scatter(noise_coordinates[:, 0], noise_coordinates[:, 1], 
                  c='gray', alpha=0.5, s=30, label='Noise points')
    
    plt.colorbar(plt.cm.ScalarMappable(cmap='viridis'), ax=ax, label='Cluster')
    plt.title('HDBSCAN Clustering - UMAP with Ellipses')
    plt.xlabel('UMAP Component 1')
    plt.ylabel('UMAP Component 2')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig('hdbscan_clusters_umap.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("HDBSCAN visualization saved as:")
    print("- hdbscan_clusters_umap.png")

def get_following_network_data(db_manager):
    """Get following network data from the database"""
    print("\n=== Getting Following Network Data ===")
    
    try:
        with sqlite3.connect(db_manager.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT insta_id, followed_creators_with_reels_selected_list
                FROM instagram_accounts 
                WHERE followed_creators_with_reels_selected_list IS NOT NULL 
                AND followed_creators_with_reels_selected_list != ''
                AND umap_x IS NOT NULL 
                AND umap_y IS NOT NULL
            """)
            
            following_data = {}
            for insta_id, followed_list_json in cursor.fetchall():
                try:
                    followed_list = json.loads(followed_list_json)
                    if isinstance(followed_list, list):
                        following_data[insta_id] = followed_list
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON for insta_id {insta_id}")
                    continue
            
            print(f"Found following data for {len(following_data)} creators")
            return following_data
            
    except Exception as e:
        logger.error(f"Error getting following network data: {e}")
        return {}

def visualize_following_network(creator_coordinates, following_data, clustering_results=None):
    """Visualize the following network with connections between creators"""
    print("\n=== Creating Following Network Visualization ===")
    
    if not creator_coordinates or not following_data:
        print("No network data to visualize")
        return
    
    # Create network graph
    G = nx.DiGraph()
    
    # Add nodes (creators) with their coordinates
    for insta_id, coords in creator_coordinates.items():
        G.add_node(insta_id, pos=coords)
    
    # Add edges (following relationships)
    edge_count = 0
    for follower_id, followed_list in following_data.items():
        if follower_id in creator_coordinates:  # Only include if follower has coordinates
            for followed_id in followed_list:
                if followed_id in creator_coordinates:  # Only include if followed has coordinates
                    G.add_edge(follower_id, followed_id)
                    edge_count += 1
    
    print(f"Created network with {G.number_of_nodes()} nodes and {edge_count} edges")
    
    if G.number_of_nodes() == 0:
        print("No valid network to visualize")
        return
    
    # Create visualization
    fig, ax = plt.subplots(figsize=(20, 16))
    
    # Get positions from coordinates
    pos = {node: creator_coordinates[node] for node in G.nodes()}
    
    # Draw the network
    if clustering_results:
        # Color nodes by cluster
        cluster_ids = sorted(set([data['cluster'] for data in clustering_results.values() if data['cluster'] != -1]))
        cmap = plt.get_cmap('viridis')
        colors = cmap(np.linspace(0, 1, len(cluster_ids)))
        
        node_colors = []
        for node in G.nodes():
            if node in clustering_results and clustering_results[node]['cluster'] != -1:
                cluster_id = clustering_results[node]['cluster']
                color_idx = cluster_ids.index(cluster_id)
                node_colors.append(colors[color_idx])
            else:
                node_colors.append('gray')
        
        # Draw nodes with cluster colors
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=100, alpha=0.8, ax=ax)
    else:
        # Draw nodes in default color
        nx.draw_networkx_nodes(G, pos, node_color='lightblue', node_size=100, alpha=0.8, ax=ax)
    
    # Draw edges with arrows
    nx.draw_networkx_edges(G, pos, edge_color='gray', alpha=0.3, arrows=True, arrowsize=10, ax=ax)
    
    # Add some node labels (for nodes with high degree)
    degrees = dict(G.degree())
    high_degree_nodes = [node for node, degree in degrees.items() if degree > 3]
    if high_degree_nodes:
        labels = {node: str(node)[:8] for node in high_degree_nodes}
        nx.draw_networkx_labels(G, pos, labels, font_size=8, font_color='black', ax=ax)
    
    plt.title('Creator Following Network (UMAP Coordinates) - HDBSCAN Clustering')
    plt.xlabel('UMAP Component 1')
    plt.ylabel('UMAP Component 2')
    plt.grid(True, alpha=0.3)
    
    # Add legend for clusters if available
    if clustering_results:
        legend_elements = []
        cluster_ids = sorted(set([data['cluster'] for data in clustering_results.values() if data['cluster'] != -1]))
        cmap = plt.get_cmap('viridis')
        colors = cmap(np.linspace(0, 1, len(cluster_ids)))
        
        for i, cluster_id in enumerate(cluster_ids):
            legend_elements.append(mlines.Line2D([0], [0], marker='o', color='w', 
                                            markerfacecolor=colors[i], markersize=10, 
                                            label=f'Cluster {cluster_id}'))
        
        legend_elements.append(mlines.Line2D([0], [0], marker='o', color='w', 
                                        markerfacecolor='gray', markersize=10, 
                                        label='Noise/Unclustered'))
        
        plt.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.savefig('creator_following_network.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("Network visualization saved as:")
    print("- creator_following_network.png")
    
    # Print network statistics
    print(f"\n=== Network Statistics ===")
    print(f"Total nodes: {G.number_of_nodes()}")
    print(f"Total edges: {G.number_of_edges()}")
    
    # Calculate average degree
    degrees = dict(G.degree())
    if degrees:
        avg_degree = sum(d for n, d in G.degree()) / G.number_of_nodes()
        print(f"Average degree: {avg_degree:.2f}")
    
    # Find most connected creators
    degree_centrality = nx.degree_centrality(G)
    top_creators = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\nTop 5 most connected creators:")
    for creator_id, centrality in top_creators:
        print(f"  Creator {creator_id}: {centrality:.3f} centrality")

def main():
    """Main function to run the clustering analysis"""
    print("=== Creator Profile Clustering Analysis ===")
    
    # Initialize database manager
    db_manager = InstagramDataManager()
    
    # Step 1: Get creator profiles from database
    creator_profiles, creator_stats = db_manager.get_creator_profiles()
    
    if not creator_profiles:
        print("No creator profiles found. Make sure embeddings have been generated.")
        return
    
    # Step 2: Generate UMAP coordinates once and save to database
    creator_coordinates, umap_result = generate_umap_coordinates(creator_profiles)
    if creator_coordinates:
        db_manager.save_umap_coordinates(creator_coordinates)
    
    # Step 3: Perform K-means clustering
    n_clusters = min(5, len(creator_profiles))  # Don't cluster more than we have profiles
    kmeans_results, kmeans_model = perform_clustering(creator_profiles, n_clusters)
    
    # Step 4: Perform HDBSCAN clustering (using Loose configuration)
    hdbscan_results, hdbscan_model = perform_hdbscan_clustering(creator_profiles)
    
    if kmeans_results and creator_coordinates:
        # Step 5: Visualize K-means results
        print(f"\n=== K-means Clustering Results ===")
        visualize_clusters(creator_coordinates, kmeans_results, n_clusters)
        
        # Step 6: Print K-means cluster analysis
        print(f"\n=== K-means Cluster Analysis ===")
        for cluster_id in range(n_clusters):
            cluster_creators = [pk for pk, data in kmeans_results.items() if data['cluster'] == cluster_id]
            print(f"Cluster {cluster_id}: {len(cluster_creators)} creators")
            
            # Show some example creators from each cluster
            for i, creator_pk in enumerate(cluster_creators[:3]):  # Show first 3
                reel_count = creator_stats[creator_pk]['reel_count']
                print(f"  - Creator {creator_pk}: {reel_count} reels")
            if len(cluster_creators) > 3:
                print(f"  ... and {len(cluster_creators) - 3} more creators")
    
    if hdbscan_results and creator_coordinates:
        # Step 7: Print HDBSCAN cluster analysis
        print(f"\n=== HDBSCAN Cluster Analysis ===")
        
        # Get unique cluster IDs (excluding noise points)
        cluster_ids = sorted(set([data['cluster'] for data in hdbscan_results.values() if data['cluster'] != -1]))
        
        for cluster_id in cluster_ids:
            cluster_creators = [pk for pk, data in hdbscan_results.items() if data['cluster'] == cluster_id]
            print(f"Cluster {cluster_id}: {len(cluster_creators)} creators")
            
            # Show some example creators from each cluster
            for i, creator_pk in enumerate(cluster_creators[:3]):  # Show first 3
                reel_count = creator_stats[creator_pk]['reel_count']
                print(f"  - Creator {creator_pk}: {reel_count} reels")
            if len(cluster_creators) > 3:
                print(f"  ... and {len(cluster_creators) - 3} more creators")
        
        # Show noise points
        noise_creators = [pk for pk, data in hdbscan_results.items() if data['is_noise']]
        if noise_creators:
            print(f"\nNoise points: {len(noise_creators)} creators")
            for i, creator_pk in enumerate(noise_creators[:5]):  # Show first 5
                reel_count = creator_stats[creator_pk]['reel_count']
                print(f"  - Creator {creator_pk}: {reel_count} reels")
            if len(noise_creators) > 5:
                print(f"  ... and {len(noise_creators) - 5} more creators")
    
    # Step 8: Save clustering results to database
    print(f"\n=== Saving Clustering Results to Database ===")
    db_manager.save_clustering_results(kmeans_results, hdbscan_results)
    
    # Step 9: Get and display clustering statistics
    clustering_stats = db_manager.get_clustering_stats()
    
    # Step 10: Visualize HDBSCAN clusters
    if creator_coordinates:
        visualize_hdbscan_clusters(creator_coordinates, hdbscan_results)

    # Step 11: Get following network data
    following_data = get_following_network_data(db_manager)
    
    # Step 12: Visualize following network with HDBSCAN clustering
    if creator_coordinates:
        visualize_following_network(creator_coordinates, following_data, hdbscan_results)

if __name__ == "__main__":
    main()
