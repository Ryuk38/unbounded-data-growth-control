import json
import os
import logging
from datetime import datetime
from fnmatch import fnmatch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PolicyEngine:
    def __init__(self, policy_file):
        """Initialize the PolicyEngine with a JSON policy file."""
        try:
            with open(policy_file, 'r') as f:
                self.policy = json.load(f)
            self._validate_policy()
            logging.info("Policy engine initialized with policy: %s", policy_file)
        except FileNotFoundError:
            logging.error("Policy file not found: %s", policy_file)
            raise
        except json.JSONDecodeError as e:
            logging.error("Invalid JSON in policy file %s: %s", policy_file, e)
            raise

    def _validate_policy(self):
        """Validate policy structure and required fields."""
        if 'rules' not in self.policy:
            raise ValueError("Policy must contain 'rules' key")

        for rule in self.policy['rules']:
            required_keys = [
                'name', 'path_match', 'retention_days',
                'hot_tier_days', 'warm_tier_action', 'cold_tier_action'
            ]
            for key in required_keys:
                if key not in rule:
                    raise ValueError(f"Rule '{rule.get('name', 'unknown')}' missing required key: {key}")

            if rule['hot_tier_days'] > rule['retention_days']:
                raise ValueError(
                    f"Rule '{rule['name']}': hot_tier_days ({rule['hot_tier_days']}) "
                    f"cannot exceed retention_days ({rule['retention_days']})"
                )

            # Default warm_tier_days if missing
            if 'warm_tier_days' not in rule:
                rule['warm_tier_days'] = rule['retention_days'] - rule['hot_tier_days']

            if rule['warm_tier_days'] < 0:
                raise ValueError(f"Rule '{rule['name']}': warm_tier_days ({rule['warm_tier_days']}) cannot be negative")

    def get_file_status(self, file_path):
        """
        Determines the current tier and required action for a given file.
        Returns None if the file doesn't exist, or a dict with tier, action, rule, and age.
        """
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            file_age_days = (datetime.now() - file_mod_time).total_seconds() / (24 * 3600)
            logging.debug("File %s: Age %.2f days", file_path, file_age_days)

            for rule in self.policy['rules']:
                if fnmatch(file_path, rule['path_match']):
                    logging.info("File %s matched rule '%s'", file_path, rule['name'])
                    
                    # *** FIX ***
                    # Create a base status dict that ALWAYS includes the age
                    base_status = {'rule': rule['name'], 'age_days': file_age_days}

                    # Expired (deletion)
                    if file_age_days > rule['retention_days']:
                        base_status.update({'tier': 'expired', 'action': 'delete'})
                        return base_status

                    # Cold tier
                    if file_age_days > rule['hot_tier_days'] + rule['warm_tier_days']:
                        base_status.update({'tier': 'cold', 'action': rule['cold_tier_action']})
                        return base_status

                    # Warm tier
                    if file_age_days > rule['hot_tier_days']:
                        base_status.update({'tier': 'warm', 'action': rule['warm_tier_action']})
                        return base_status

                    # Hot tier
                    base_status.update({'tier': 'hot', 'action': 'none'})
                    return base_status

            logging.warning("File %s did not match any rule", file_path)
            # *** FIX ***
            return {'tier': 'unmatched', 'action': 'none', 'rule': 'none', 'age_days': file_age_days}

        except FileNotFoundError:
            logging.error("File not found: %s", file_path)
            return None # main.py already checks for None
        except Exception as e:
            logging.error("Error processing file %s: %s", file_path, e)
            # *** FIX ***
            return {'tier': 'error', 'action': 'none', 'rule': 'none', 'error': str(e), 'age_days': 0}


# Example Usage
if __name__ == "__main__":
    engine = PolicyEngine("policy.json")
    # This example usage will fail now unless you create this file
    # print(engine.get_file_status("/var/log/app/service-xyz.log")) 
    print("PolicyEngine class loaded and validated.")