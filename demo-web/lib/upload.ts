import { createServerFn } from "@tanstack/react-start";
import { mkdir, writeFile } from "fs/promises";
import { join } from "path";

/** Saves a base64-encoded image to public/uploads/ and returns the URL path. */
export const uploadImageServer = createServerFn({ method: "POST" })
  .inputValidator((data: { base64: string; filename: string; mimeType: string }) => data)
  .handler(async ({ data }): Promise<{ url: string }> => {
    const { base64, filename, mimeType } = data;

    // Validate mime type
    const allowed = ["image/jpeg", "image/png", "image/webp", "image/gif"];
    if (!allowed.includes(mimeType)) {
      throw new Error("Only JPEG, PNG, WebP, and GIF images are supported.");
    }

    // Sanitise filename and add timestamp to avoid collisions
    const ext = filename.split(".").pop()?.toLowerCase() ?? "jpg";
    const safeName = filename
      .replace(/[^a-zA-Z0-9._-]/g, "-")
      .replace(/\.+/g, ".")
      .slice(0, 80);
    const timestamp = Date.now();
    const uniqueName = `${timestamp}-${safeName}`;

    // Resolve public/uploads relative to the process cwd (project root at runtime)
    const uploadsDir = join(process.cwd(), "public", "uploads");
    await mkdir(uploadsDir, { recursive: true });

    const buffer = Buffer.from(base64, "base64");
    const filePath = join(uploadsDir, uniqueName);
    await writeFile(filePath, buffer);

    return { url: `/uploads/${uniqueName}` };
  });
