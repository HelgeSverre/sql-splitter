const EXPLICIT_ID = /\s+\{#([A-Za-z][A-Za-z0-9:._-]*)\}$/;

function visit(node) {
  if (node?.type === "heading") {
    const last = node.children?.at(-1);
    if (last?.type === "text") {
      const match = last.value.match(EXPLICIT_ID);
      if (match) {
        last.value = last.value.slice(0, match.index);
        node.data ??= {};
        node.data.hProperties ??= {};
        node.data.hProperties.id = match[1];
      }
    }
  }

  for (const child of node?.children ?? []) visit(child);
}

/** Preserve explicitly declared, case-sensitive Markdown heading IDs. */
export default function remarkExplicitHeadingIds() {
  return (tree) => visit(tree);
}
