import {
  useRef,
  useEffect,
  useImperativeHandle,
  forwardRef,
  type Ref,
} from "react";
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightSpecialChars } from "@codemirror/view";
import { EditorState, Compartment } from "@codemirror/state";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { searchKeymap, highlightSelectionMatches } from "@codemirror/search";
import { bracketMatching, syntaxHighlighting, defaultHighlightStyle } from "@codemirror/language";
import { MSSQL, PostgreSQL, sql } from "@codemirror/lang-sql";
import { autocompletion, type CompletionSource } from "@codemirror/autocomplete";
import { oneDark } from "@codemirror/theme-one-dark";

export interface SqlEditorHandle {
  getValue: () => string;
  replaceAll: (text: string) => void;
  view: EditorView | null;
}

interface SqlEditorProps {
  dialect?: string; // "mssql" | "postgres"
  completionSource?: CompletionSource | null;
  onExecute?: () => void;
  placeholder?: string;
}

const dialectCompartment = new Compartment();
const completionCompartment = new Compartment();
const themeCompartment = new Compartment();

function getDialect(d?: string) {
  return d === "postgres" ? PostgreSQL : MSSQL;
}

function isDarkMode() {
  return document.documentElement.classList.contains("dark");
}

const SqlEditor = forwardRef(function SqlEditor(
  { dialect, completionSource, onExecute, placeholder }: SqlEditorProps,
  ref: Ref<SqlEditorHandle>,
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onExecuteRef = useRef(onExecute);
  onExecuteRef.current = onExecute;

  useImperativeHandle(ref, () => ({
    getValue: () => viewRef.current?.state.doc.toString() ?? "",
    replaceAll: (text: string) => {
      const view = viewRef.current;
      if (!view) return;
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: text },
      });
    },
    get view() {
      return viewRef.current;
    },
  }));

  // Create editor on mount
  useEffect(() => {
    if (!containerRef.current) return;

    const dark = isDarkMode();

    const executeKeymap = keymap.of([
      {
        key: "Ctrl-Enter",
        mac: "Cmd-Enter",
        run: () => {
          onExecuteRef.current?.();
          return true;
        },
      },
    ]);

    const state = EditorState.create({
      doc: "",
      extensions: [
        lineNumbers(),
        highlightActiveLine(),
        highlightSpecialChars(),
        history(),
        bracketMatching(),
        highlightSelectionMatches(),
        syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
        keymap.of([...defaultKeymap, ...historyKeymap, ...searchKeymap]),
        executeKeymap,
        dialectCompartment.of(sql({ dialect: getDialect(dialect) })),
        completionCompartment.of(
          completionSource
            ? autocompletion({ override: [completionSource] })
            : autocompletion(),
        ),
        themeCompartment.of(dark ? oneDark : []),
        EditorView.theme({
          "&": { height: "192px", fontSize: "14px" },
          ".cm-scroller": { overflow: "auto" },
          ".cm-content": { fontFamily: "ui-monospace, monospace" },
        }),
        placeholder
          ? EditorView.contentAttributes.of({ "aria-label": placeholder })
          : [],
      ],
    });

    const view = new EditorView({ state, parent: containerRef.current });
    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Reconfigure dialect when it changes
  useEffect(() => {
    viewRef.current?.dispatch({
      effects: dialectCompartment.reconfigure(sql({ dialect: getDialect(dialect) })),
    });
  }, [dialect]);

  // Reconfigure completions when source changes
  useEffect(() => {
    viewRef.current?.dispatch({
      effects: completionCompartment.reconfigure(
        completionSource
          ? autocompletion({ override: [completionSource] })
          : autocompletion(),
      ),
    });
  }, [completionSource]);

  // Watch for dark mode changes
  useEffect(() => {
    const observer = new MutationObserver(() => {
      const dark = isDarkMode();
      viewRef.current?.dispatch({
        effects: themeCompartment.reconfigure(dark ? oneDark : []),
      });
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  return (
    <div
      ref={containerRef}
      className="border rounded-md overflow-hidden resize-y"
      data-testid="sql-editor"
    />
  );
});

export default SqlEditor;
