// ====== Налаштування ======
const API_KEY = '3102164cc1c31d21ed8b77a42a695ba9';
const BASE_URL = 'https://api.themoviedb.org/3';
const IMG_URL = 'https://image.tmdb.org/t/p/w342';

const { fromEvent, merge, of, EMPTY, Observable } = rxjs;
const { debounceTime, distinctUntilChanged, switchMap, map, filter, tap, catchError } = rxjs.operators;

// ====== Елементи DOM ======
const input = document.querySelector('#search-input');
const searchBtn = document.querySelector('#search-btn');
const searchBox = document.querySelector('.search-box');
const suggestionsList = document.querySelector('#suggestions');
const statusEl = document.querySelector('#status');
const resultsEl = document.querySelector('#results');
const dialog = document.querySelector('#details-dialog');
const dialogBody = document.querySelector('#dialog-body');

// ====== Власний клас помилки API ======
class ApiError extends Error {
  constructor(message, status = 0, retryAfter = null) {
    super(message);
    this.status = status;
    this.retryAfter = retryAfter;
  }
}

// ====== Кеш відповідей (менше запитів = менший ризик rate limit) ======
const cache = new Map();

// ====== Асинхронний запит до TMDb (fetch + async/await) ======
async function tmdb(path, params = {}, signal) {
  if (API_KEY.startsWith('ВСТАВТЕ')) {
    throw new ApiError('Вставте свій API-ключ TMDb у файл app.js', 401);
  }
  const url = new URL(BASE_URL + path);
  url.search = new URLSearchParams({ api_key: API_KEY, language: 'en-US', ...params }).toString();
  const key = url.toString();
  if (cache.has(key)) return cache.get(key);

  const response = await fetch(key, { signal });

  // fetch не кидає помилку на 404/429/500, тому статус перевіряємо вручну
  if (response.status === 429) {
    throw new ApiError('Забагато запитів до TMDb.', 429, response.headers.get('Retry-After'));
  }
  if (response.status === 401) throw new ApiError('Невірний API-ключ TMDb.', 401);
  if (response.status === 404) throw new ApiError('Нічого не знайдено (404).', 404);
  if (!response.ok) throw new ApiError(`Помилка сервера TMDb (${response.status}).`, response.status);

  const data = await response.json();
  cache.set(key, data);
  return data;
}

// Обгортка запиту в Observable: при switchMap попередній fetch скасовується
function request$(path, params) {
  return new Observable((subscriber) => {
    const controller = new AbortController();
    tmdb(path, params, controller.signal)
      .then((data) => { subscriber.next(data); subscriber.complete(); })
      .catch((err) => subscriber.error(err));
    return () => controller.abort();
  });
}

// ====== Допоміжні функції ======
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function setStatus(text, type = '') {
  statusEl.textContent = text;
  statusEl.className = 'status ' + type;
}

function errorText(err) {
  if (err.name === 'AbortError') return '';
  if (err instanceof ApiError) {
    if (err.status === 429) {
      const wait = err.retryAfter ? `через ${err.retryAfter} с` : 'трохи згодом';
      return `${err.message} Спробуйте ${wait}.`;
    }
    return err.message;
  }
  return 'Не вдалося з’єднатися із сервером. Перевірте інтернет-з’єднання.';
}

function showError(err) {
  const text = errorText(err);
  if (text) setStatus(text, 'error');
}

const isMedia = (r) => r.media_type === 'movie' || r.media_type === 'tv';

const toItem = (r) => ({
  id: r.id,
  type: r.media_type,
  title: r.title || r.name,
  year: (r.release_date || r.first_air_date || '').slice(0, 4) || '—',
  rating: r.vote_average ? r.vote_average.toFixed(1) : '—',
  poster: r.poster_path,
});

function posterEl(path, title) {
  if (!path) return el('div', 'no-poster', 'Немає постера');
  const img = el('img');
  img.src = IMG_URL + path;
  img.alt = title;
  img.loading = 'lazy';
  return img;
}

// ====== Виведення результатів ======
function renderResults(items, query) {
  resultsEl.replaceChildren();
  if (items.length === 0) {
    setStatus(`Нічого не знайдено за запитом «${query}».`, 'error');
    return;
  }
  setStatus(`Знайдено: ${items.length}`);
  items.forEach((item) => {
    const card = el('article', 'card');
    card.dataset.id = item.id;
    card.dataset.type = item.type;
    card.append(posterEl(item.poster, item.title));
    const info = el('div', 'card-info');
    info.append(el('h3', '', item.title));
    info.append(el('p', '', `${item.year} • ${item.type === 'tv' ? 'Серіал' : 'Фільм'}`));
    info.append(el('p', '', `⭐ ${item.rating}`));
    card.append(info);
    resultsEl.append(card);
  });
}

function hideSuggestions() {
  suggestionsList.hidden = true;
  suggestionsList.replaceChildren();
}

function renderSuggestions(items) {
  suggestionsList.replaceChildren();
  if (items.length === 0) { suggestionsList.hidden = true; return; }
  items.forEach((item) => {
    const li = el('li', '', `${item.title} (${item.year})`);
    li.dataset.title = item.title;
    suggestionsList.append(li);
  });
  suggestionsList.hidden = false;
}

// ====== RxJS: автозаповнення (debounce 300 мс) ======
fromEvent(input, 'input').pipe(
  map((e) => e.target.value.trim()),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap((query) => {
    if (query.length < 2) return of([]);
    return request$('/search/multi', { query }).pipe(
      map((data) => data.results.filter(isMedia).slice(0, 6).map(toItem)),
      catchError((err) => { showError(err); return of([]); })
    );
  })
).subscribe(renderSuggestions);

// ====== RxJS: основний пошук (кнопка або Enter) ======
merge(
  fromEvent(searchBtn, 'click'),
  fromEvent(input, 'keydown').pipe(filter((e) => e.key === 'Enter'))
).pipe(
  map(() => input.value.trim()),
  tap(hideSuggestions),
  switchMap((query) => {
    if (!query) {
      setStatus('Введіть назву для пошуку.', 'error');
      return EMPTY;
    }
    setStatus('Завантаження...', 'loading');
    resultsEl.replaceChildren();
    return request$('/search/multi', { query }).pipe(
      map((data) => ({ query, items: data.results.filter(isMedia).map(toItem) })),
      catchError((err) => { showError(err); return EMPTY; })
    );
  })
).subscribe(({ query, items }) => renderResults(items, query));

// ====== Вибір підказки ======
suggestionsList.addEventListener('click', (event) => {
  const li = event.target.closest('li');
  if (!li) return;
  input.value = li.dataset.title;
  searchBtn.click();
});

document.addEventListener('click', (event) => {
  if (!searchBox.contains(event.target)) hideSuggestions();
});

// ====== Деталі фільму за кліком (делегування) ======
resultsEl.addEventListener('click', (event) => {
  const card = event.target.closest('.card');
  if (card) openDetails(card.dataset.type, card.dataset.id);
});

async function openDetails(type, id) {
  dialogBody.replaceChildren(el('p', 'status loading', 'Завантаження...'));
  dialog.showModal();
  try {
    // append_to_response: актори приходять в одному запиті (економить ліміт)
    const data = await tmdb(`/${type}/${id}`, { append_to_response: 'credits' });
    if (!data.overview) {
      const en = await tmdb(`/${type}/${id}`, { language: 'en-US' });
      data.overview = en.overview;
    }
    renderDetails(data, type);
  } catch (err) {
    dialogBody.replaceChildren(el('p', 'status error', errorText(err)));
  }
}

function renderDetails(d, type) {
  const title = d.title || d.name;
  const year = (d.release_date || d.first_air_date || '').slice(0, 4) || '—';
  const wrap = el('div', 'details');
  wrap.append(posterEl(d.poster_path, title));

  const info = el('div', 'details-info');
  info.append(el('h2', '', `${title} (${year})`));
  info.append(el('p', '', `${type === 'tv' ? 'Серіал' : 'Фільм'} • ⭐ ${d.vote_average ? d.vote_average.toFixed(1) : '—'}`));
  info.append(el('p', '', `Жанри: ${d.genres.map((g) => g.name).join(', ') || '—'}`));
  info.append(el('p', '', d.overview || 'Опис відсутній.'));
  const cast = d.credits?.cast?.slice(0, 8).map((c) => c.name).join(', ');
  info.append(el('p', '', `Актори: ${cast || '—'}`));
  wrap.append(info);
  dialogBody.replaceChildren(wrap);
}

// ====== Закриття вікна деталей ======
document.querySelector('#close-btn').addEventListener('click', () => dialog.close());
dialog.addEventListener('click', (event) => {
  if (event.target === dialog) dialog.close();
});