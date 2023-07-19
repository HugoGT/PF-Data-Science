function enviarForm(event) {
  event.preventDefault(); // Evita que el formulario se envíe de manera tradicional

  window.open("https://wa.me/14155238886?text=join%20stream-post", "_newtab");

  // Obtener los valores del formulario
  const body = JSON.stringify({
    nombre: event.target.name.value,
    email: event.target.email.value,
    celular: event.target.celular.value,
    ubicacion: event.target.ubicacion.value,
  });
  // Realizar la solicitud a la API utilizando fetch
  fetch("https://sismos-qagw.onrender.com/guardar_usuario", {
    body,
    method: "post",
    headers: {
      "Content-Type": "application/json",
    },
  })
    .then((response) => response.json())
    .then((data) => {
      // Aquí puedes manejar la respuesta de la API
      console.log(data);

      // Obtener el mensaje de éxito de la respuesta de la API
      const mensajeExito = data.message;

      // Mostrar el mensaje de éxito al usuario (por ejemplo, en un elemento HTML)
      const feedbackElement = document.getElementById("feedback");
      feedbackElement.innerText = mensajeExito;

      // Mostrar el div de feedback y vaciar su contenido
      feedbackElement.style.display = "block";
      feedbackElement.innerText = "";

      // Agregar el nuevo mensaje de éxito al div de feedback
      const mensajeElement = document.createElement("p");
      mensajeElement.innerText = mensajeExito;
      feedbackElement.appendChild(mensajeElement);
    })
    .catch((error) => {
      // Manejo de errores si ocurre algún problema en la solicitud
      console.error("Error:", error);
    });
}

const form = document.getElementById("form");
form.addEventListener("submit", enviarForm);
