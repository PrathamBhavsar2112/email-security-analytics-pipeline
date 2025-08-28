import json

input_file = "Email-Enron.txt"
output_file = "email-Enron.json"

edges = []

with open(input_file, "r", encoding="utf-8") as f:
    for raw in f:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue 
        parts = line.split()
        if len(parts) != 2:
            continue
        from_node, to_node = parts
        edges.append({"from": f"user{from_node}", "to": f"user{to_node}"})

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(edges, f, indent=2, ensure_ascii=False)

print(f"Converted {input_file} to {output_file} with {len(edges)} edges.")
