/** Lee una variable de entorno obligatoria. */
export function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Falta la variable de entorno ${name}`);
  return v;
}

export const envInt = (name: string, fallback: number): number => {
  const n = Number(process.env[name]);
  return Number.isFinite(n) && process.env[name] !== undefined && process.env[name] !== '' ? n : fallback;
};

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
