from Worker import CacheProcessor, Matcher, TreeReducer

# Divide grid into standard size chunks...

# Launch CacheProcessor instance

# Send chunk size to CacheProcessor

# Fill cache storage with templates

# For each chunk:

    # Fill cache with curvature grids

    # Save chunk on EFS volume in /efs/data/

    # Divide up parameter space into k subsets

    # Launch k Matcher instances

    # Launch TreeReducer instance

    # Reduce results
