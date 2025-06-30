# Aesthetic Clustering and Social Network Analysis Toolkit

[![Alt text](https://cdn.240.agency/atlas.svg)](https://atlas.240.agency/)

The page as contains codebase for *"Interpreting Aesthetic Communities: A Dаtа-Driven Study of Homophily on Instаgrаm Reels"*. We set an aim to explore how do аesthetic similаrities shаpe sociаl connectivity on Instаgrаm Reels, аnd how they reveаl homophily mechаnisms within the plаtform.

**Key concepts:** `Semantic map`, `Network`, `Latent Representations`, `Unsupervised Clustering`, `Platform Vernaculars`.

# Architecture of the project

At the heart of the project lies our SQLite **database**, which contains all information the project interacts with. 
**Location:** `data/instagram_data.db`.
It is generated on first start with the help of `db_manager` manager.

The project is composed of main.py, as well other supplumentary modules with self-explanatory names:

- `music`
- `speech`
- `video`
- `vector`
- and others.

Each module handles a distinct processing stage.

Recommended and the only avaliable pipeline for this project is linear execution of these individual scripts from entry point. This decision was made mainly for two reasons.

To keep the script-like feel of the project - not to bury in queues, asynchronicity, and job schedulers that will blur the research interest and shift towards endless overengineering. This approach provides for readability and easy refactoring, as well as sturdy ground for maintenance.

The second reason is abundance of memory-heavy operations, e.g. at this current point project get's use of 4 models ([Whisper](https://github.com/openai/whisper), [LLama](https://huggingface.co/meta-llama/Llama-3.1-8B), [LLava](https://huggingface.co/llava-hf/LLaVA-NeXT-Video-7B-DPO-hf), [All-Mpnet-Base-v2](https://huggingface.co/sentence-transformers/all-mpnet-base-v2)). Memory consistency would require a redundant engeering complexity.

> Another important limitation that the current version is only compatible with **Linux-based systems**. Unfortunantly, the core efficiency library - [flash-attention](https://github.com/Dao-AILab/flash-attention) - is natively only compatible with GCC (GNU Compiler Collection). As a result, compatibility on Windows is headache.

# Summary of Data and Analysis

### Description of the Data

The foundation for this study is a hand-assembled dataset of **401 Europe-based Instagram influencers**. Analysis is based on the following data layers:

| User (pseudo)        | Data          |
| -------------------  |:-------------:|
| id                   | 946543        |
| followers            | 145 000       |
| following            | 525           |
| selected reels       | r1, r2, r3    |
| connection with      | user1, user2  |
| cords-x      | 0.391342  |
| cords-y      | 0.934384  |
| cluster      | 0.934384  |

The user is the cornerstone of the tool. They are carefully stored at `instagram_accounts` table. There are some other fields, though it's not relevant at this point.

---

| Reel (pseudo)          | Data          |
| -------------------  |:-------------:|
| id                   | 1231234        |
| author            | 946543       |
| audio_type            | speech           |
| audio_content       | my friends    |
| model description      | Cheerful crowd  |
| model embedding      | [0,343], ..., [0,213]  |
| content url      | https://...  |

Reel is the workhorse of the project. The most diverse and complex interactions are done in `reels` table.

| Following (pseudo)       | Data          |
| -------------------  |:-------------:|
| id                   | 1231234        |
| follows            | 946543       |

Followers, in their turn, take the biggest part of the database. With just 401 creators fetched, there are approximately 250 000 rows in `following` table. Though, it is neccessary to store them, considering that newly added creators need to be integrated into the network.

## Code Workflow and Analysis Process

As mentioned, the analysis is structured as a modular pipeline where each script has own task. They are done consequently and changes are applied to the whole dataset. However, each file can act as an entry point itself. Considering, researcher wants to offload the computationally expensive video inference to a GPU-cluster, it becomes a pleasure to run just `video.py`.
<!-- data_manager.upsert_account(
    username=user.get("username", ""),
    insta_id=pk,
    follower_count=user.get("follower_count", 0),
    following_count=following_count,
    full_name=user.get("full_name", ""),
    url=f"https://www.instagram.com/{user.get('username', '')}/",
    profile_pic_url=profile_pic_url,
    biography=user.get("biography", "")
) -->

## Script Breakdown

#### 0. **Sample input**

`add_new_users.py`, `test.py`, `db_manager.py`

- The first step in a sequence of generating a semantic map is to provide data. In our case, this was a `.csv` file containing a list of Instagrams (@instagram, @facebook...).  
- `add_new_users` allows to read that file from `/data/` and to add them for further metadata collection.  
- Another option is to run `test`. The script would create a database and fill it with new testing entities. This is useful in debugging or exploring the capabilities of the script.  
- Some may prefer using SQL request(s) to fill in the data, which is also afforded by robust design.  
- A private fork contents API request to Atlas to fetch newly acquired addition requests.  
- Any means to provide URL are possible and wouldn't affect the further processes.  

---

#### 1. **Data extraction**

`hiker.py`

- The script iterates through provided Instagrams and fetches using a third-party data provider service HikerAPI SaaS.  
- Earlier, this part of the pipeline was held by a bot management system. However, this process has shown lack of efficiency due to complexity.  
- The following data is collected by default:  
  - `Min Followers > 10 000` — to explicitly check if there are no empty accounts in the dataset.  
  - `Max Following < 1 000` — avoids long fetches of following, API respect  
  - `Reels to fetch = 60` — the most viewed reels are selected among X latest posts  
- Instagram scraper has its rules defined in `.env` to avoid analyzing unnecessary people elements.  
- This script is built with high fault tolerance. It logs its actions to prevent excessive API calls. Numerous explicit checks ensure no data is left unfulfilled.  

---

#### 2. **Download**

`download_reels.py`

- The download system provides user with robust design. Downloaded videos are marked in the database, providing stable synchronization of requested and received data.  
- The videos are downloaded to `data/reels`  
- By default, thumbnails are downloaded to `data/thumbnails`  
- Beware that CDN typically serves within a limited period of time, usually requiring immediate download.  

---

#### 3. **Audio pre-processing**

`utility/extract_audio.py`, `music.py`, `speech.py`

- Audio of each downloaded reel is extracted using `ffmpeg`.  
- The audio is examined whether it contains music (using `ACRCloud`) or speech (using `Whisper`).  
  - `ACRCloud` provides wide free capabilities and its own Python SDK.  
  - `Whisper` detects the language and automatically transcribes content in English.  
- Audio analysis provides a wider textual context for aesthetic analysis.  

---

#### 4. **Text pre-processing**

`concise.py`

- Script evaluates the length of audio transcription, as well as caption by the author.  
- It has its own settings in `.env`, allowing control over the degree of shortening.  
- This is done to increase the possible video context by reducing text length.  

---

#### 5. **Aesthetic Feature Extraction**

`video.py`

- The core of the project — `video.py` — utilizes `llava-hf/LLaVA-NeXT-Video-7B-DPO-hf`, a model to generate a description of each reel's aesthetic.  
- The system prompt set is carefully adjusted to provide better results.  
- The script uses **scene-based frame sampling**, choosing a few representative frames rather than processing the entire video.  
- Frames and related text (speech transcription / music, caption) are used to trigger the model to generate a comprehensive description.  
- Instead of LLaVA, it's also possible to use an embedded model, lowering a loss due to switching modalities.  
  - [ImageBind](https://github.com/facebookresearch/ImageBind) provides a comprehensive model for this purpose, as well some CLIP models  
- For cheaper inference quantization is possible and applied by default.  

    ```python
    quantization_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
    )
    ```

  - This allows to run LLaVA on GPUs with limited memory without loss of performance for this task.


---

#### 6. **Vectorization**  

`postvideo.py`.`vector.py`

- If there is no direct mention of "video", the results are more vivid. Creator coordinates are more widely placed on the map, `postvideo.py` cleans the results of the inference for better embeddings.
- LLaVA-NeXT text descriptions are encoded into high-dimensional numerical vectors (embeddings) using `sentence-transformers/all-mpnet-base-v2`.  
- These embeddings capture semantic information contained in the aesthetic descriptions.  

---

#### 7. **Clustering & Profile Generation**  

`clustering.py`

- For each creator's 5 reels, embeddings are **averaged** into a single **aesthetic profile** vector.  
  - `UMAP` is used for dimensionality reduction and for vectors for visualization.  
  - `HDBSCAN`, a density-based clustering method, groups artists without predefining the number of clusters.  
    - Has an experimental mode, allowing to choose the best clustering for provided data
  - `PCA \ T-SNE` can also be applied but did not perform well in our case due to importance of local structure (to preserve clusters)
  - In the current version, Gaussian Ellipses are printed to highlight the denseast area of the cluster. Does not have any research significane, however, allows pleasant experience of surveying the map.

<p align="center">
  <img src="https://cdn.240.agency/t-sne.jpg" alt="Alt text" width="800"/>
</p>

---

#### 8. **Hypothesis Testing**

`social_connections.py`, `hypothesis_testing.py`

- The final script integrates social network data and cluster assignments.
- Analysis block is capable of conveying various stastical analysis, specifically tailored for individial use cases.
- Current configuration uses a **Permutation Test** to test H1, and **Spearman Correlation** tests to test H2 and H3 — establishing statistical evidence for the paper’s conclusions.

The goal was to test three primary hypotheses of aesthetic homophily:

- **H1:** Members of a same-cluster group follow each other more than would be expected by random chance.
- **H2:** Those creators more similar to the aesthetic of their cluster have higher clustering confidence scores.
- **H3:** "Bridge" designers, who are positioned aesthetically between a number of clusters, have lower clustering confidence.

However, this part is up to a final user. He database is filled with data at this points and numerous other researchs of Instagram communities becomes possible.

#### 9. **Deployment**

`Dockerfile`

- A `Dockerfile` docker was specifically added to access GPU-cluster. Though, some researchers might find container a more convinient experience.

## Presentation of Results

The results of this analysis are presented in several different ways:

- **Statistical Summaries:** The `hypothesis_testing.py` script produces metrics for current hypothesis and corresponding statistical measurements (p-value and correlation coefficient).

- **Static Visualizations:** The script `clustering.py` generates two significant plots:
  - `hdbscan_clusters_umap.png`: A 2D scatter plot of the aesthetic clusters.
  - `creator_following_network.png`: A plot of the social network between creators, color-coded by aesthetic cluster.

- **Interactive Visualization:** For the interactive version of the data and clusters, go to the interactive project site: **[atlas.240.agency](https://atlas.240.agency/)**.

## Setup and Usage

### Docker Deployment

This is the best manner of running the entire project because it provides a consistent environment.

1. **Recommended prerequisites:**
    - Linux
    - Support for Cuda 12.8
    - 24 VRAM (minimum for non-quantized version of LLaVa)

2. **Clone the Repository:**
    ```bash
    git clone https://github.com/molchalih/inst2txt.git
    cd inst2txt

    ```

3. **Data Location:**
    - Place your `data` directory (with the original CSV, and subdirectories for reels, audio, etc.) at the root of the `inst2txt` project.
    - Include a `.env` file in the root directory for any needed API keys (e.g., `ACRCLOUD_ACCESS_KEY`).

4. **Create Docker Image:**
    ```bash
    docker build -t inst2txt-app .
    ```

5. **Running the Container:**
    - The following command runs the container, mounting local folders for data and model caches to prevent data loss and model re-downloads each time it is run.

    ```bash
    docker run --gpus all --rm -it \
      -v "$(pwd)/data:/app/data" \
      -v "$(pwd)/model_cache:/app/model_cache" \
      -v "$(pwd)/llava-processor:/app/llava-processor" \
      -v "$(pwd)/sessions:/app/sessions" \
      --env-file .env \
      inst2txt-app
    ```

Best of luck!
**[atlas.240.agency](https://atlas.240.agency/)**
